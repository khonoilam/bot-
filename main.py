import asyncio
import os
import time
import logging
import secrets
import pytz
import json
import threading
import requests
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from flask import Flask
from threading import Thread

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.constants import ParseMode

# ======================== Flask keep‑alive ========================
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot đang chạy!"

def web():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

Thread(target=web, daemon=True).start()

# ======================== CONFIG ========================
BOT_TOKEN = "8258187122:AAF0qdhmiuX29smYszqFIgrs_iVgWF_rpUo"
ADMIN_ID = 8721023843

# TangAcc API
TANGACC_TOKEN = "https://tangacc.net/token.php"
TANGACC_ACC = "https://tangacc.net/get_lq_acc.php"
THREADS = 50  # số thread scan đồng thời
TIMEOUT = 10  # timeout HTTP

# Check acc API
CHECK_API_URL = "http://160.22.107.245:5000/check"
CHECK_API_KEY = "RSAEJ8rdtRaMLfVUCB70Mh8pL0SFSDDx"

# JSONBin
JSONBIN_API_KEY = "$2a$10$ZKItx9kCcaQktuLuBDKY1ewYhT2gy3OWH.w7nkeTLWUy9sCxtjVWO"
JSONBIN_BIN_ID = "6a6b41e8da38895dfea40e00"
JSONBIN_API_URL = "https://api.jsonbin.io/v3/b"

# Limits
LIMIT_NORMAL = 100          # acc/ngày cho key thường
LIMIT_VIP = 550             # acc/ngày cho key VIP
CHECK_LIMIT_NORMAL = 25     # check/ngày cho key thường
CHECK_LIMIT_VIP = 75        # check/ngày cho key VIP
MAX_PER_REQ = 50
KEY_EXPIRE_DAYS = 30

# Timezone & anti‑spam
TZ = pytz.timezone("Asia/Ho_Chi_Minh")
SPAM_WINDOW = 10
MAX_SPAM = 5
MUTE_TIME = 60

# Thư mục lưu file xuất
OUTPUT_DIR = "lq_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ======================== LOGGING ========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger(__name__)

# ======================== GLOBAL DATA ========================
keys = {}               # key chưa dùng
users = {}              # thông tin người dùng
used_accounts = set()   # tất cả acc đã phát hành
started_users = set()   # tất cả user đã từng /start (để admin broadcast)
last_acc_list = {}      # uid -> list acc strings vừa lấy, dùng để check

# ======================== HEADERS ========================
HEADERS = {
    "authority": "tangacc.net",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://tangacc.net/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# ======================== JSONBIN SYNC ========================
def load_data():
    global keys, users, used_accounts, started_users
    try:
        r = requests.get(
            f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}/latest",
            headers={"X-Master-Key": JSONBIN_API_KEY},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json().get("record", {})
            keys = data.get("keys", {})
            users = data.get("users", {})
            used_accounts = set(data.get("used_accounts", []))
            started_users = set(data.get("started_users", []))
            log.info(f"Đã tải bin: {len(keys)} keys, {len(users)} users")
            return
    except Exception as e:
        log.error(f"Lỗi tải bin: {e}")
    keys, users, used_accounts, started_users = {}, {}, set(), set()

def save_data():
    try:
        data = {
            "keys": keys,
            "users": users,
            "used_accounts": list(used_accounts),
            "started_users": list(started_users),
        }
        requests.put(
            f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}",
            json=data,
            headers={
                "X-Master-Key": JSONBIN_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=10,
        )
    except Exception as e:
        log.error(f"Lỗi lưu bin: {e}")

# ======================== TANGACC ENGINE ========================
def get_acc(session):
    try:
        r = session.get(TANGACC_TOKEN, headers=HEADERS, timeout=TIMEOUT)
        token = r.text.strip()
        if not token:
            return None
        h = {
            **HEADERS,
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://tangacc.net",
        }
        r2 = session.post(TANGACC_ACC, headers=h, data={"token": token}, timeout=TIMEOUT)
        acc = r2.text.strip()
        if acc and not acc.startswith("WAIT") and "|" in acc:
            return acc
    except Exception:
        pass
    return None

def fetch_fast(n):
    acc_set = set()
    live_accs = []
    lock = threading.Lock()
    stop_flag = threading.Event()

    def worker():
        session = requests.Session()
        fail_count = 0
        while not stop_flag.is_set() and len(live_accs) < n:
            acc = get_acc(session)
            if not acc:
                fail_count += 1
                if fail_count > 30:
                    break
                continue
            fail_count = 0
            with lock:
                if acc in acc_set or acc in used_accounts:
                    continue
                acc_set.add(acc)
                used_accounts.add(acc)
                live_accs.append(acc)

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        for _ in range(THREADS):
            ex.submit(worker)
        deadline = time.time() + 30  # tối đa 30 giây
        while len(live_accs) < n and time.time() < deadline:
            time.sleep(0.3)
        stop_flag.set()

    log.info(f"Lấy {len(live_accs)} acc trong {time.time()-time.time():.1f}s")
    return live_accs[:n]

# ======================== CHECK ACC LOGIC ========================
def check_acc_api(username, password):
    try:
        r = requests.post(
            CHECK_API_URL,
            data={"user": username, "pass": password, "apikey": CHECK_API_KEY},
            timeout=30,
        )
        d = r.json()
        if d.get("ok") and d["result"].get("status") == "HIT":
            info = d["result"]
            skins = info.get("aov_skins", {})
            return {
                "status": "HIT",
                "username": username,
                "name": info.get("aov_name"),
                "uid": info.get("uid"),
                "rank": info.get("aov_rank"),
                "level": info.get("aov_level"),
                "skins": skins.get("total_skins", 0),
                "champs": skins.get("total_champs", 0),
                "banned": info.get("aov_banned"),
                "fb_linked": info.get("fb_linked"),
                "mobile_bound": info.get("mobile_bound"),
                "email_verified": info.get("email_verified"),
            }
        else:
            return {
                "status": "MISS",
                "username": username,
                "message": d.get("result", {}).get("detail", "Sai mật khẩu / không tồn tại"),
            }
    except Exception as e:
        return {"status": "ERROR", "username": username, "message": str(e)}

def get_check_limit(uid):
    if uid == ADMIN_ID:
        return 99999
    user = users.get(str(uid), {})
    return CHECK_LIMIT_VIP if user.get("vip") else CHECK_LIMIT_NORMAL

def get_remaining_checks(uid):
    if uid == ADMIN_ID:
        return 99999
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    user = users.get(str(uid), {})
    last_date = user.get("last_check_date")
    if last_date != today:
        return get_check_limit(uid)
    return max(0, get_check_limit(uid) - user.get("checked_today", 0))

def update_check_count(uid, count):
    uid = str(uid)
    if uid not in users:
        return
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    user = users[uid]
    if user.get("last_check_date") != today:
        user["last_check_date"] = today
        user["checked_today"] = count
    else:
        user["checked_today"] = user.get("checked_today", 0) + count
    save_data()

# ======================== UTILS ========================
request_log = defaultdict(list)
muted_users = {}
processing_users = set()

def today_vn():
    return datetime.now(TZ).strftime("%Y-%m-%d")

def is_admin(uid):
    return uid == ADMIN_ID

def is_auth(uid):
    if uid == ADMIN_ID:
        return True
    return str(uid) in users and users[str(uid)].get("key") is not None

def get_limit(uid):
    if uid == ADMIN_ID:
        return 99999
    return LIMIT_VIP if users.get(str(uid), {}).get("vip") else LIMIT_NORMAL

def get_user(uid):
    if uid == ADMIN_ID:
        if "admin" not in users:
            users["admin"] = {"history": [], "used": 0, "daily_used": 0, "banned": False, "vip": True}
        return users["admin"]
    return users.get(str(uid), {})

def check_spam(uid):
    if uid == ADMIN_ID:
        return False
    now = time.time()
    if uid in muted_users:
        if now < muted_users[uid]:
            return True
        else:
            del muted_users[uid]
    request_log[uid] = [t for t in request_log[uid] if now - t < SPAM_WINDOW]
    request_log[uid].append(now)
    if len(request_log[uid]) > MAX_SPAM:
        muted_users[uid] = now + MUTE_TIME
        log.warning(f"Mute {uid}")
        return True
    return False

def main_menu(uid):
    u = get_user(uid)
    limit = get_limit(uid)
    used = u.get("daily_used", 0)
    remaining = limit - used
    vip = "👑 VIP" if u.get("vip") else "🔑 Thường"
    check_rem = get_remaining_checks(uid)
    keyboard = [
        [InlineKeyboardButton("🎮 Lấy 10 Acc", callback_data="lay_10"),
         InlineKeyboardButton("🎮 Lấy 30 Acc", callback_data="lay_30")],
        [InlineKeyboardButton("🎮 Lấy 50 Acc", callback_data="lay_50")],
        [InlineKeyboardButton("📁 Xuất File", callback_data="export"),
         InlineKeyboardButton("👤 Profile", callback_data="profile")],
        [InlineKeyboardButton("🔑 Nhập Key", callback_data="key_input")],
    ]
    text = (
        f"🤖 *LQ ACC BOT*\n{vip}\n"
        f"📊 Hôm nay: {used}/{limit} (Còn {remaining})\n"
        f"🔍 Check: {check_rem} lượt\n"
        "⚡TangAcc\nChọn chức năng:"
    )
    return InlineKeyboardMarkup(keyboard), text

def admin_menu_text():
    return (
        "👑 *ADMIN MENU*\n\n"
        "/genkey - Tạo key thường\n"
        "/genvip - Tạo key VIP\n"
        "/status - Trạng thái key & user\n"
        "/users - Danh sách người dùng\n"
        "/stats - Thống kê\n"
        "/keys - Key chưa dùng\n"
        "/tb <nội dung> - Gửi thông báo đến tất cả\n"
        "/muted - Danh sách mute\n"
        "/unmute <id> - Bỏ mute\n"
        "/ban <id> - Khóa người dùng\n"
        "/unban <id> - Mở khóa\n"
        "/revoke <id> - Thu hồi key\n"
        "/reset <id> - Reset lượt dùng\n"
        "/delkey <key> - Xóa key\n"
        "/resetall CONFIRM - Xóa toàn bộ dữ liệu"
    )

def login_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Nhập Key", callback_data="key_input")],
        [InlineKeyboardButton("🔗 Lấy Key Free", url="https://t.me/chantuiii")],
    ])

# ======================== HANDLERS ========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != ADMIN_ID and check_spam(uid):
        await update.message.reply_text("🚫 Bạn đã bị cấm chat 60 giây vì spam!")
        return
    user = update.effective_user
    name = user.full_name or user.username or str(uid)
    if str(uid) in users:
        users[str(uid)]["name"] = name
        save_data()
    started_users.add(str(uid))  # lưu lại để broadcast
    if uid == ADMIN_ID:
        await update.message.reply_text(admin_menu_text(), parse_mode=ParseMode.MARKDOWN)
    elif is_auth(uid):
        kb, text = main_menu(uid)
        await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(
            "🤖 *LQ ACC BOT*\n\n"
            "🔐 Bạn chưa có key để sử dụng bot.\n\n"
            "👉 Nhấn nút bên dưới để *Lấy Key Free*\n"
            "   (ib t.me/chantuiii để nhận key)\n\n"
            "Nếu đã có key, nhấn *Tôi đã có Key* để nhập.",
            reply_markup=login_menu(),
            parse_mode=ParseMode.MARKDOWN,
        )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data

    if not is_auth(uid) and data not in ["key_input"]:
        await query.edit_message_text("🔐 Bạn chưa nhập key!", reply_markup=login_menu())
        return
    if check_spam(uid):
        await query.edit_message_text("🚫 Bạn đã bị cấm chat 60 giây vì spam!")
        return

    # --- Lấy acc (lay) ---
    if data.startswith("lay_"):
        n = int(data.split("_")[1])
        u = get_user(uid)
        limit = get_limit(uid)
        if u.get("banned"):
            await query.edit_message_text("🚫 Bạn đã bị khóa!")
            return
        if u.get("date") != today_vn():
            u["date"] = today_vn()
            u["daily_used"] = 0
        if uid != ADMIN_ID:
            remaining = limit - u.get("daily_used", 0)
            if remaining <= 0:
                await query.edit_message_text(f"🚫 Hết {limit} acc/ngày\n/key <mã>")
                return
            if n > remaining:
                n = remaining
        if n > MAX_PER_REQ:
            n = MAX_PER_REQ
        if uid in processing_users:
            await query.edit_message_text("⏳ Đợi...")
            return
        processing_users.add(uid)
        await query.edit_message_text(f"⏳ Đang lấy {n} acc...")
        t0 = time.time()
        loop = asyncio.get_event_loop()
        accs = await loop.run_in_executor(None, fetch_fast, n)
        t1 = time.time() - t0
        processing_users.discard(uid)
        if not accs:
            await query.edit_message_text("❌ Không lấy được acc.")
            return
        # Cập nhật lịch sử
        u["history"] = u.get("history", []) + accs
        u["used"] = u.get("used", 0) + len(accs)
        u["daily_used"] = u.get("daily_used", 0) + len(accs)
        if uid != ADMIN_ID:
            save_data()
        # Lưu danh sách vừa lấy để check
        last_acc_list[uid] = accs.copy()
        # Tạo nút check
        check_rem = get_remaining_checks(uid)
        check_buttons = []
        for x in [5, 10, 15, 20, 25]:
            if x <= check_rem:
                check_buttons.append(InlineKeyboardButton(f"Check {x}", callback_data=f"checkacc_{x}"))
        # Hiển thị acc + nút check
        acc_text = "\n".join(accs[:20])  # tối đa 20 dòng
        if len(accs) > 20:
            acc_text += f"\n... và {len(accs)-20} acc khác"
        msg = f"🎉 {len(accs)} acc ({t1:.1f}s)\n\n{acc_text}"
        reply_markup = None
        if check_buttons:
            # Chia thành 2 hàng
            row1 = check_buttons[:3]
            row2 = check_buttons[3:]
            keyboard = []
            if row1:
                keyboard.append(row1)
            if row2:
                keyboard.append(row2)
            reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(msg, reply_markup=reply_markup)
        # Gửi menu chính
        kb, text = main_menu(uid)
        await context.bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return

    # --- Check acc ---
    if data.startswith("checkacc_"):
        parts = data.split("_")
        if len(parts) != 2:
            return
        try:
            req_count = int(parts[1])
        except ValueError:
            return
        uid = update.effective_user.id
        if uid not in last_acc_list or not last_acc_list[uid]:
            await query.edit_message_text("⚠️ Vui lòng lấy acc trước để sử dụng tính năng check.")
            return
        acc_list = last_acc_list[uid]
        remaining = get_remaining_checks(uid)
        can_check = min(req_count, len(acc_list), remaining)
        if can_check <= 0:
            await query.edit_message_text("🚫 Bạn đã hết lượt check hôm nay hoặc không có acc để check.")
            return
        # Tiến hành check
        await query.edit_message_text(f"🔍 Đang check {can_check} acc...")
        accs_to_check = acc_list[:can_check]
        results = []
        for acc_str in accs_to_check:
            if "|" not in acc_str:
                continue
            user, pw = acc_str.split("|", 1)
            res = check_acc_api(user, pw)
            results.append(res)

        # Cập nhật lượt check
        update_check_count(uid, len(accs_to_check))
        # Soạn kết quả
        hit = [r for r in results if r["status"] == "HIT"]
        miss = [r for r in results if r["status"] != "HIT"]
        text = f"📊 Kết quả check {len(results)} acc\n"
        text += f"✅ HIT: {len(hit)}\n❌ MISS/ERROR: {len(miss)}\n"
        if hit:
            text += "\n*Chi tiết HIT:*\n"
            for r in hit[:5]:  # chỉ show 5 acc hit
                text += (
                    f"👤 `{r['username']}` - {r.get('name','?')}\n"
                    f"   Rank: {r.get('rank','?')} | Lv: {r.get('level','?')}\n"
                    f"   Skin: {r.get('skins',0)} | Tướng: {r.get('champs',0)}\n"
                )
        if miss:
            text += "\n*MISS:*\n"
            for r in miss[:3]:
                text += f"❌ `{r['username']}` - {r.get('message','?')}\n"
        # Nút quay lại menu
        kb, _ = main_menu(uid)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return

    # --- Export ---
    if data == "export":
        u = get_user(uid)
        history = u.get("history", [])
        if not history:
            await query.edit_message_text("📭 Chưa lấy acc nào.")
            return
        ts = int(time.time())
        fn = f"{OUTPUT_DIR}/history_{uid}_{ts}.txt"
        with open(fn, "w", encoding="utf-8") as f:
            f.write("\n".join(history))
        with open(fn, "rb") as f:
            await context.bot.send_document(
                chat_id=uid,
                document=f,
                filename=f"lich_su_acc_{ts}.txt",
                caption=f"📁 {len(history)} acc",
            )
        await query.edit_message_text("✅ Đã gửi file!")
        return

    # --- Profile ---
    if data == "profile":
        u = get_user(uid)
        limit = get_limit(uid)
        vip = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
        check_rem = get_remaining_checks(uid)
        await query.edit_message_text(
            f"👤 {vip}\n"
            f"📊 Lấy: {u.get('daily_used',0)}/{limit}\n"
            f"📦 Tổng: {u.get('used',0)}\n"
            f"🔍 Check: {check_rem} lượt còn lại\n"
            f"📋 Lịch sử: {len(u.get('history',[]))} acc"
        )
        return

    # --- Nhập key ---
    if data == "key_input":
        await query.edit_message_text("📝 Gửi key của bạn: /key <mã>")
        return

# Lệnh /key
async def key_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args:
        await update.message.reply_text("🔐 Key không đúng! Nhấn nút Lấy Key Free để được cấp key.")
        return
    key = context.args[0].strip().upper()
    if key not in keys:
        await update.message.reply_text("🔐 Key không đúng!")
        return
    kd = keys[key]
    if kd.get("banned"):
        await update.message.reply_text("❌ Key bị khóa")
        return
    if time.time() - kd.get("created_ts", 0) > KEY_EXPIRE_DAYS * 86400:
        await update.message.reply_text("❌ Key hết hạn")
        return
    is_vip = kd.get("vip", False)
    users[uid] = {
        "key": key,
        "vip": is_vip,
        "used": 0,
        "daily_used": 0,
        "date": today_vn(),
        "banned": False,
        "history": [],
        "last_check_date": today_vn(),
        "checked_today": 0,
        "name": update.effective_user.full_name or uid,
    }
    del keys[key]
    save_data()
    limit = LIMIT_VIP if is_vip else LIMIT_NORMAL
    kb, text = main_menu(int(uid))
    await update.message.reply_text(
        f"✅ {'👑 VIP' if is_vip else '🔑 Thường'} | {limit} acc/ngày\nCheck: {CHECK_LIMIT_VIP if is_vip else CHECK_LIMIT_NORMAL} acc/ngày",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )

# Các lệnh còn lại giữ nguyên (chỉ thêm /tb)
async def lay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_auth(uid):
        await update.message.reply_text("🔐 Chưa nhập key\nNhấn nút Lấy Key Free để được cấp key.")
        return
    if check_spam(uid):
        await update.message.reply_text("🚫 Bạn đã bị cấm chat 60 giây vì spam!")
        return
    u = get_user(uid)
    if u.get("banned"):
        await update.message.reply_text("🚫 Bị khóa")
        return
    limit = get_limit(uid)
    if u.get("date") != today_vn():
        u["date"] = today_vn()
        u["daily_used"] = 0
    try:
        n = int(context.args[0]) if context.args else 10
    except ValueError:
        n = 10
    if uid != ADMIN_ID:
        remaining = limit - u.get("daily_used", 0)
        if n > MAX_PER_REQ:
            n = MAX_PER_REQ
        if n > remaining:
            await update.message.reply_text(f"⚠️ Còn {remaining} acc")
            return
        if remaining <= 0:
            await update.message.reply_text(f"🚫 Hết acc\n/key <mã>")
            return
    processing_users.add(uid)
    msg = await update.message.reply_text(f"⏳ {n} acc...")
    loop = asyncio.get_event_loop()
    accs = await loop.run_in_executor(None, fetch_fast, n)
    processing_users.discard(uid)
    if not accs:
        await msg.edit_text("Không lấy được")
        return
    u["history"] = u.get("history", []) + accs
    u["used"] = u.get("used", 0) + len(accs)
    u["daily_used"] = u.get("daily_used", 0) + len(accs)
    if uid != ADMIN_ID:
        save_data()
    last_acc_list[uid] = accs.copy()
    # Nút check
    check_rem = get_remaining_checks(uid)
    check_buttons = []
    for x in [5, 10, 15, 20, 25]:
        if x <= check_rem:
            check_buttons.append(InlineKeyboardButton(f"Check {x}", callback_data=f"checkacc_{x}"))
    acc_text = "\n".join(accs[:20])
    if len(accs) > 20:
        acc_text += f"\n... và {len(accs)-20} acc khác"
    text = f"🎉 {len(accs)} acc\n\n{acc_text}"
    reply_markup = None
    if check_buttons:
        keyboard = [check_buttons[i:i+3] for i in range(0, len(check_buttons), 3)]
        reply_markup = InlineKeyboardMarkup(keyboard)
    await msg.edit_text(text, reply_markup=reply_markup)

# /tb (broadcast) – admin only
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("📝 /tb <nội dung>")
        return
    text = " ".join(context.args)
    success = 0
    fail = 0
    for uid_str in list(started_users):
        try:
            await context.bot.send_message(chat_id=int(uid_str), text=text)
            success += 1
        except Exception:
            fail += 1
    await update.message.reply_text(f"✅ Đã gửi: {success} người\n❌ Lỗi: {fail}")

async def genkey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 20:
        n = 20
    new_keys = []
    for _ in range(n):
        k = secrets.token_hex(8).upper()
        keys[k] = {"type": "normal", "created": datetime.now(TZ).strftime("%d/%m/%Y %H:%M"), "created_ts": time.time()}
        new_keys.append(k)
    save_data()
    await update.message.reply_text(f"🔑 Key thường ({LIMIT_NORMAL} acc/ngày):\n" + "\n".join(f"`{k}`" for k in new_keys), parse_mode=ParseMode.MARKDOWN)

async def genvip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 10:
        n = 10
    new_keys = []
    for _ in range(n):
        k = secrets.token_hex(8).upper()
        keys[k] = {"type": "vip", "vip": True, "created": datetime.now(TZ).strftime("%d/%m/%Y %H:%M"), "created_ts": time.time()}
        new_keys.append(k)
    save_data()
    await update.message.reply_text(f"👑 Key VIP ({LIMIT_VIP} acc/ngày):\n" + "\n".join(f"`{k}`" for k in new_keys), parse_mode=ParseMode.MARKDOWN)

async def key_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    text = "📊 *Trạng thái Key*\n\n"
    if keys:
        text += "🔑 *Chưa dùng:*\n"
        for k, v in list(keys.items())[:15]:
            text += f"`{k}` ({'VIP' if v.get('vip') else 'TH'})\n"
    if users:
        text += "\n👥 *Đã dùng:*\n"
        for uid, u in list(users.items())[:20]:
            if uid in ("admin", str(ADMIN_ID)):
                continue
            name = u.get("name", uid)
            key_short = u.get("key", "?")[:8]
            vip = "VIP" if u.get("vip") else "TH"
            daily = u.get("daily_used", 0)
            limit = get_limit(int(uid))
            banned = "🚫" if u.get("banned") else ""
            text += f"👤 {name} | ID:`{uid}` | Key:{key_short}... ({vip}) | {daily}/{limit} {banned}\n"
    if not keys and not users:
        text += "📭 Chưa có dữ liệu"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not users:
        await update.message.reply_text("📭 Chưa có user")
        return
    text = "👥 *Danh sách User:*\n\n"
    for uid, u in list(users.items())[:30]:
        if uid in ("admin", str(ADMIN_ID)):
            continue
        name = u.get("name", uid)
        key_short = u.get("key", "?")[:8]
        vip = "VIP" if u.get("vip") else "TH"
        daily = u.get("daily_used", 0)
        limit = get_limit(int(uid))
        banned = "🚫" if u.get("banned") else "✅"
        text += f"👤 {name} | `{uid}` | {key_short}... ({vip}) | {daily}/{limit} {banned}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    nk = sum(1 for k in keys.values() if not k.get("vip"))
    vk = sum(1 for k in keys.values() if k.get("vip"))
    total = sum(u.get("used", 0) for u in users.values())
    await update.message.reply_text(f"📊 Key: {nk}+{vk}VIP | 👥 {len(users)} | 📦 {total} | 📋 {len(used_accounts)} | 🤐 {len(muted_users)}")

async def listkeys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not keys:
        await update.message.reply_text("📭 Kho key trống")
        return
    text = "🔑 *Kho key:*\n"
    for k, v in list(keys.items())[:20]:
        text += f"`{k}` ({'VIP' if v.get('vip') else 'TH'})\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("/ban <id>")
        return
    uid = context.args[0]
    if uid in users:
        users[uid]["banned"] = True
        save_data()
        await update.message.reply_text(f"✅ Đã ban {uid}")
    else:
        await update.message.reply_text("❌ Không tìm thấy")

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("/unban <id>")
        return
    uid = context.args[0]
    if uid in users:
        users[uid]["banned"] = False
        save_data()
        await update.message.reply_text(f"✅ Đã unban {uid}")
    else:
        await update.message.reply_text("❌ Không tìm thấy")

async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("/revoke <id>")
        return
    uid = context.args[0]
    if uid in users:
        del users[uid]
        save_data()
        await update.message.reply_text(f"✅ Đã thu hồi key của {uid}")
    else:
        await update.message.reply_text("❌ Không tìm thấy")

async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if context.args:
        uid = context.args[0]
        if uid in users:
            users[uid]["daily_used"] = 0
            users[uid]["date"] = today_vn()
            save_data()
            await update.message.reply_text(f"✅ Đã reset {uid}")
        else:
            await update.message.reply_text("❌ Không tìm thấy")
    else:
        for u in users:
            users[u]["daily_used"] = 0
            users[u]["date"] = today_vn()
        save_data()
        await update.message.reply_text("✅ Đã reset tất cả")

async def delkey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("/delkey <key>")
        return
    key = context.args[0].strip().upper()
    if key in keys:
        del keys[key]
        save_data()
        await update.message.reply_text("✅ Đã xóa key")
    else:
        await update.message.reply_text("❌ Key không tồn tại hoặc đã dùng")

async def muted_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not muted_users:
        await update.message.reply_text("📭 Không có ai bị mute.")
        return
    text = "🤐 *Danh sách Mute:*\n\n"
    now = time.time()
    for uid, until in list(muted_users.items())[:20]:
        remaining = int(until - now)
        if remaining > 0:
            name = get_user(int(uid)).get("name", str(uid))
            text += f"👤 {name} | `{uid}` | ⏳ {remaining}s\n"
    if text == "🤐 *Danh sách Mute:*\n\n":
        text += "📭 Không có ai bị mute."
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def unmute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("/unmute <id>")
        return
    uid = int(context.args[0])
    if uid in muted_users:
        del muted_users[uid]
        request_log[uid].clear()
        await update.message.reply_text(f"✅ Đã bỏ mute {uid}")
    else:
        await update.message.reply_text(f"❌ {uid} không bị mute")

async def resetall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args or context.args[0] != "CONFIRM":
        await update.message.reply_text("⚠️ Xác nhận: /resetall CONFIRM")
        return
    keys.clear()
    users.clear()
    used_accounts.clear()
    request_log.clear()
    muted_users.clear()
    started_users.clear()
    save_data()
    log.warning("RESET ALL!")
    await update.message.reply_text("☢️ Đã xóa TOÀN BỘ dữ liệu!")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log.exception(context.error)

# ======================== MAIN ========================
if __name__ == "__main__":
    log.info(f"🤖 LQ ACC BOT | Admin:{ADMIN_ID} | TangAcc + Check")
    load_data()
    application = Application.builder().token(BOT_TOKEN).build()
    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("key", key_cmd))
    application.add_handler(CommandHandler("lay", lay))
    application.add_handler(CommandHandler("tb", broadcast))
    application.add_handler(CommandHandler("genkey", genkey))
    application.add_handler(CommandHandler("genvip", genvip))
    application.add_handler(CommandHandler("status", key_status))
    application.add_handler(CommandHandler("users", users_list))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("keys", listkeys))
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))
    application.add_handler(CommandHandler("revoke", revoke))
    application.add_handler(CommandHandler("reset", reset))
    application.add_handler(CommandHandler("delkey", delkey))
    application.add_handler(CommandHandler("muted", muted_list))
    application.add_handler(CommandHandler("unmute", unmute_cmd))
    application.add_handler(CommandHandler("resetall", resetall))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.run_polling(drop_pending_updates=True)
