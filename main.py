import asyncio, os, time, logging, secrets, pytz, json, threading, requests, copy
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

# ======================== FLASK ========================
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot đang chạy!"

def web():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# ======================== CONFIG ========================
BOT_TOKEN = "8258187122:AAExWr8i1jAeqZJbxnWkLW39gGA_FQN3I1I"
ADMIN_ID = 8721023843
TANGACC_TOKEN = "https://tangacc.net/token.php"
TANGACC_ACC   = "https://tangacc.net/get_lq_acc.php"
THREADS = 70
TIMEOUT = 10
CHECK_API_URL = "http://160.22.107.245:5000/check"
CHECK_API_KEY = "RSAEJ8rdtRaMLfVUCB70Mh8pL0SFSDDx"
JSONBIN_API_KEY = "$2a$10$ZKItx9kCcaQktuLuBDKY1ewYhT2gy3OWH.w7nkeTLWUy9sCxtjVWO"
JSONBIN_BIN_ID = "6a6b41e8da38895dfea40e00"
JSONBIN_API_URL = "https://api.jsonbin.io/v3/b"
LIMIT_NORMAL = 100
LIMIT_VIP = 550
CHECK_LIMIT_NORMAL = 25
CHECK_LIMIT_VIP = 75
MAX_PER_REQ = 50
KEY_EXPIRE_SECONDS = 86400
TZ = pytz.timezone("Asia/Ho_Chi_Minh")
SPAM_WINDOW = 10
MAX_SPAM = 5
MUTE_TIME = 60
CHECK_TIMEOUT = 8
CHECK_CONCURRENT = 100
OUTPUT_DIR = "lq_data"
MAX_HISTORY = 2000
MAX_LAST_ACC = 500
MAX_CHECKED_ACC = 500

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
log = logging.getLogger(__name__)

# ======================== LOCKS ========================
keys_lock = threading.Lock()
users_lock = threading.Lock()
acc_lock = threading.Lock()
spam_lock = threading.Lock()
data_lock = threading.Lock()
processing_lock = threading.Lock()

# ======================== GLOBAL DATA ========================
keys = {}
users = {}
used_accounts = set()

HEADERS = {
    "authority": "tangacc.net", "accept": "*/*", "accept-language": "en-US,en;q=0.9",
    "referer": "https://tangacc.net/", "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ======================== UTILS ========================
def today_vn():
    return datetime.now(TZ).strftime("%Y-%m-%d")

def is_admin(uid):
    return uid == ADMIN_ID

# ======================== INTERNAL HELPERS ========================
def _clean_expired_keys_nolock():
    now = time.time()
    expired = [k for k, v in keys.items() if now - v.get("created_ts", 0) > KEY_EXPIRE_SECONDS]
    for k in expired:
        del keys[k]
    if expired:
        log.info(f"Đã xóa {len(expired)} key hết hạn")
    return bool(expired)

def _get_user_copy(uid):
    if uid == ADMIN_ID:
        if "admin" not in users:
            users["admin"] = {"history":[],"used":0,"daily_used":0,"banned":False,"vip":True,"last_acc":[],"checked_acc":[]}
        return copy.deepcopy(users["admin"])
    u = users.get(str(uid))
    if u:
        return copy.deepcopy(u)
    return {}

# ======================== JSONBIN ========================
def load_data():
    global keys, users, used_accounts
    with data_lock:
        try:
            r = requests.get(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}/latest",
                             headers={"X-Master-Key": JSONBIN_API_KEY}, timeout=10)
            if r.status_code == 200:
                data = r.json().get("record", {})
                with keys_lock:
                    keys = data.get("keys", {})
                    _clean_expired_keys_nolock()
                with users_lock:
                    users = data.get("users", {})
                with acc_lock:
                    used_accounts = set(data.get("used_accounts", []))
                log.info(f"Đã tải bin: {len(keys)} keys, {len(users)} users")
                return
        except Exception as e:
            log.error(f"Lỗi tải bin: {e}")

def save_data():
    with data_lock:
        for attempt in range(3):
            try:
                with keys_lock:
                    k = copy.deepcopy(keys)
                with users_lock:
                    u = copy.deepcopy(users)
                with acc_lock:
                    a = list(used_accounts)
                payload = {"keys": k, "users": u, "used_accounts": a}
                r = requests.put(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}", json=payload,
                                 headers={"X-Master-Key": JSONBIN_API_KEY, "Content-Type": "application/json"},
                                 timeout=10)
                if r.status_code == 200:
                    log.info("Lưu bin thành công")
                    return
                else:
                    log.error(f"Lỗi lưu bin lần {attempt+1}: {r.status_code}")
            except Exception as e:
                log.error(f"Lỗi lưu bin: {e}")
            time.sleep(1)

# ======================== TANGACC ENGINE ========================
def get_acc(session):
    try:
        r = session.get(TANGACC_TOKEN, headers=HEADERS, timeout=TIMEOUT)
        token = r.text.strip()
        if not token: return None
        h = {**HEADERS, "content-type": "application/x-www-form-urlencoded", "origin": "https://tangacc.net"}
        r2 = session.post(TANGACC_ACC, headers=h, data={"token": token}, timeout=TIMEOUT)
        acc = r2.text.strip()
        if acc and "|" in acc and not acc.startswith("WAIT") and not acc.startswith("ERROR"):
            return acc
    except Exception as e:
        log.error(f"Lỗi get_acc: {e}")
    return None

def fetch_fast(n):
    acc_set = set()
    live_accs = []
    lock = threading.Lock()
    stop_flag = threading.Event()

    def worker():
        with requests.Session() as session:
            fail_count = 0
            while not stop_flag.is_set():
                with lock:
                    if len(live_accs) >= n:
                        break
                acc = get_acc(session)
                if not acc:
                    fail_count += 1
                    if fail_count > 15:
                        break
                    continue
                fail_count = 0
                with lock:
                    if len(live_accs) >= n:
                        break
                    if acc in acc_set:
                        continue
                    with acc_lock:
                        if acc in used_accounts:
                            continue
                        used_accounts.add(acc)
                    acc_set.add(acc)
                    live_accs.append(acc)

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        for _ in range(THREADS):
            ex.submit(worker)
        deadline = time.time() + 45
        while len(live_accs) < n and time.time() < deadline:
            time.sleep(0.3)
        stop_flag.set()
    return live_accs[:n]

# ======================== CHECK ACC (SIÊU TỐC) ========================
check_executor = ThreadPoolExecutor(max_workers=200)

def check_acc_api(username, password):
    try:
        r = requests.post(CHECK_API_URL,
                          data={"user": username, "pass": password, "apikey": CHECK_API_KEY},
                          timeout=CHECK_TIMEOUT)
        r.raise_for_status()
        d = r.json()
        if d.get("ok") and d["result"].get("status") == "HIT":
            info = d["result"]
            skins = info.get("aov_skins", {})
            ban_info = info.get("aov_banned", "NO")
            ban_start = info.get("ban_start", "")
            ban_end = info.get("ban_end", "")
            if isinstance(ban_info, dict):
                ban_start = ban_info.get("start", ban_start)
                ban_end = ban_info.get("end", ban_end)
                ban_status = "YES" if ban_info.get("status") == "banned" else "NO"
            else:
                ban_status = ban_info
            return {
                "status": "HIT", "username": username, "name": info.get("aov_name"),
                "uid": info.get("uid"), "rank": info.get("aov_rank"), "level": info.get("aov_level"),
                "skins": skins.get("total_skins", 0), "champs": skins.get("total_champs", 0),
                "banned": ban_status, "ban_start": ban_start, "ban_end": ban_end,
                "fb_linked": info.get("fb_linked"), "mobile_bound": info.get("mobile_bound"),
                "email_verified": info.get("email_verified"),
            }
        else:
            return {"status": "MISS", "username": username,
                    "message": d.get("result", {}).get("detail", "Sai mật khẩu / không tồn tại")}
    except Exception as e:
        return {"status": "ERROR", "username": username, "message": str(e)}

async def check_acc_async(username, password):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(check_executor, check_acc_api, username, password)

# ======================== SPAM ========================
request_log = defaultdict(list)
muted_users = {}

def check_spam(uid):
    if uid == ADMIN_ID: return False
    now = time.time()
    with spam_lock:
        if uid in muted_users:
            if now < muted_users[uid]:
                return True
            else:
                del muted_users[uid]
        times = request_log[uid]
        times = [t for t in times if now - t < SPAM_WINDOW]
        times.append(now)
        request_log[uid] = times
        if len(times) > MAX_SPAM:
            muted_users[uid] = now + MUTE_TIME
            return True
    return False

# ======================== MENUS ========================
def main_menu(uid):
    with users_lock:
        u = _get_user_copy(uid)
        if uid == ADMIN_ID:
            limit = 99999
            check_rem = 99999
        else:
            limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL
            today = today_vn()
            if u.get("last_check_date") != today:
                check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
            else:
                check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today", 0))
        used = u.get("daily_used", 0)
        remaining = max(0, limit - used)
        vip_text = "👑 VIP" if u.get("vip") else "🔑 Thường"
    keyboard = [
        [InlineKeyboardButton("🎮 Lấy 10 Acc", callback_data="lay_10"),
         InlineKeyboardButton("🎮 Lấy 30 Acc", callback_data="lay_30")],
        [InlineKeyboardButton("🎮 Lấy 50 Acc", callback_data="lay_50")],
        [InlineKeyboardButton("📁 Xuất Acc Thô", callback_data="export"),
         InlineKeyboardButton("📁 Xuất Acc Check", callback_data="export_checked")],
        [InlineKeyboardButton("👤 Profile", callback_data="profile")],
    ]
    text = (f"🤖 *LQ ACC BOT*\n{vip_text}\n"
            f"📊 Hôm nay: {used}/{limit} (Còn {remaining})\n"
            f"🔍 Check: {check_rem} lượt\n"
            "⚡TangAcc\nChọn chức năng:")
    return InlineKeyboardMarkup(keyboard), text

def admin_menu_text():
    return ("👑 *ADMIN MENU*\n\n"
            "/genkey - Tạo key thường (24h)\n"
            "/genvip - Tạo key VIP (24h)\n"
            "/status - Trạng thái key & user\n"
            "/users - Danh sách người dùng\n"
            "/stats - Thống kê\n"
            "/keys - Key chưa dùng\n"
            "/muted - Danh sách mute\n"
            "/unmute <id> - Bỏ mute\n"
            "/ban <id> - Khóa người dùng\n"
            "/unban <id> - Mở khóa\n"
            "/revoke <id> - Thu hồi key\n"
            "/reset <id> - Reset lượt dùng\n"
            "/delkey <key> - Xóa key\n"
            "/resetall CONFIRM - Xóa toàn bộ dữ liệu")

def login_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Nhập Key", callback_data="key_input")],
        [InlineKeyboardButton("🔗 Lấy Key Free", url="https://t.me/chantuiii")],
    ])

# ======================== HANDLERS ========================
processing_users = set()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != ADMIN_ID and check_spam(uid):
        await update.message.reply_text("🚫 Spam! 60s.")
        return
    user = update.effective_user
    name = user.full_name or user.username or str(uid)
    need_save = False
    with users_lock:
        if str(uid) in users:
            if users[str(uid)].get("name") != name:
                users[str(uid)]["name"] = name
                need_save = True
    if need_save:
        save_data()
    if uid == ADMIN_ID:
        await update.message.reply_text(admin_menu_text(), parse_mode=ParseMode.MARKDOWN)
    else:
        with users_lock:
            auth = (str(uid) in users and users[str(uid)].get("key") is not None)
        if auth:
            kb, text = main_menu(uid)
            await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("🤖 *LQ ACC BOT*\n\n🔐 Chưa có key.\n👉 Nhấn nút dưới.",
                                            reply_markup=login_menu(), parse_mode=ParseMode.MARKDOWN)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data

    with users_lock:
        auth = (uid == ADMIN_ID) or (str(uid) in users and users[str(uid)].get("key") is not None)
        if not auth and data != "key_input":
            await query.edit_message_text("🔐 Bạn chưa nhập key!", reply_markup=login_menu())
            return
        if auth:
            u_copy = _get_user_copy(uid)
            if u_copy.get("banned"):
                await query.edit_message_text("🚫 Bạn đã bị khóa!")
                return

    if check_spam(uid):
        await query.edit_message_text("🚫 Spam!")
        return

    if data.startswith("lay_"):
        n = int(data.split("_")[1])
        if uid != ADMIN_ID and n > MAX_PER_REQ:
            await query.answer(f"Tối đa {MAX_PER_REQ} acc/lần.", show_alert=True)
            n = MAX_PER_REQ

        with processing_lock:
            if uid in processing_users:
                await query.edit_message_text("⏳ Đợi...")
                return
            processing_users.add(uid)

        try:
            with users_lock:
                if uid == ADMIN_ID:
                    limit = 99999
                else:
                    u = users.get(str(uid))
                    if not u:
                        await query.edit_message_text("Lỗi xác thực.")
                        return
                    if u.get("date") != today_vn():
                        u["date"] = today_vn()
                        u["daily_used"] = 0
                    limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL
                    remaining = limit - u.get("daily_used", 0)
                    if remaining <= 0:
                        await query.edit_message_text(f"🚫 Hết {limit} acc/ngày")
                        return
                    if n > remaining:
                        n = remaining
                need_save = (uid != ADMIN_ID)

            await query.edit_message_text(f"⏳ Đang lấy {n} acc...")
            t0 = time.time()
            loop = asyncio.get_event_loop()
            accs = await loop.run_in_executor(None, fetch_fast, n)
            t1 = time.time() - t0
            if not accs:
                await query.edit_message_text("❌ Không lấy được acc.")
                return

            # GHI ĐÈ last_acc
            with users_lock:
                if uid == ADMIN_ID:
                    u = users.setdefault("admin", {"history":[],"used":0,"daily_used":0,"banned":False,"vip":True,"last_acc":[],"checked_acc":[]})
                else:
                    u = users[str(uid)]
                hist = u.get("history", [])
                hist = (hist + accs)[-MAX_HISTORY:]
                u["history"] = hist
                u["used"] = u.get("used", 0) + len(accs)
                u["daily_used"] = u.get("daily_used", 0) + len(accs)
                u["last_acc"] = accs.copy()
                last_copy = list(accs)
                total_acc = len(accs)

            if need_save:
                save_data()

            with users_lock:
                if uid == ADMIN_ID:
                    check_rem = 99999
                else:
                    u = users[str(uid)]
                    today = today_vn()
                    if u.get("last_check_date") != today:
                        check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
                    else:
                        check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today", 0))
            check_buttons = [InlineKeyboardButton(f"✅ Check {x}", callback_data=f"checkacc_{x}")
                             for x in [5,10,15,20,25] if x <= check_rem and x <= total_acc]
            acc_text = "\n".join(last_copy[:20])
            if total_acc > 20:
                acc_text += f"\n... và {total_acc-20} acc khác"
            msg = f"🎉 {len(accs)} acc ({t1:.1f}s)\n\n{acc_text}"
            if len(msg) > 4000:
                msg = msg[:4000] + "\n... (cắt bớt)"
            reply_markup = InlineKeyboardMarkup([check_buttons[i:i+3] for i in range(0, len(check_buttons), 3)]) if check_buttons else None
            await query.edit_message_text(msg, reply_markup=reply_markup)
        finally:
            with processing_lock:
                processing_users.discard(uid)

    elif data.startswith("checkacc_"):
        parts = data.split("_")
        if len(parts) != 2: return
        req_count = int(parts[1])
        with processing_lock:
            if uid in processing_users:
                await query.answer("Đang xử lý, vui lòng đợi.", show_alert=True)
                return
            processing_users.add(uid)
        try:
            with users_lock:
                u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {"last_acc":[]})
                acc_list = list(u.get("last_acc", []))
                if not acc_list:
                    await query.edit_message_text("⚠️ Vui lòng lấy acc trước để sử dụng tính năng check.")
                    return
                today = today_vn()
                if u.get("last_check_date") != today:
                    remaining = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
                else:
                    remaining = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today", 0))
                can_check = min(req_count, len(acc_list), remaining)
                if can_check <= 0:
                    await query.edit_message_text("🚫 Bạn đã hết lượt check hôm nay hoặc không có acc để check.")
                    return
                accs_to_check = acc_list[:can_check]

            await query.edit_message_text(f"🔍 Đang check {can_check} acc...")
            t0 = time.time()
            sem = asyncio.Semaphore(CHECK_CONCURRENT)
            async def limited_check(acc_str):
                if "|" not in acc_str:
                    return {"status": "ERROR", "username": acc_str, "message": "Sai định dạng"}
                uname, pwd = acc_str.split("|", 1)
                async with sem:
                    return await check_acc_async(uname, pwd)
            tasks = [limited_check(acc) for acc in accs_to_check]
            results = await asyncio.gather(*tasks)
            t1 = time.time() - t0

            success_results = [r for r in results if r["status"] in ("HIT", "MISS")]
            hit_acc_strs = [accs_to_check[i] for i, r in enumerate(results) if r["status"] == "HIT"]

            with users_lock:
                u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {"history":[],"last_acc":[],"checked_acc":[],"last_check_date":"","checked_today":0})
                if success_results:
                    today = today_vn()
                    if u.get("last_check_date") != today:
                        u["last_check_date"] = today
                        u["checked_today"] = len(success_results)
                    else:
                        u["checked_today"] = u.get("checked_today", 0) + len(success_results)
                    checked = u.get("checked_acc", [])
                    checked.extend(hit_acc_strs)
                    if len(checked) > MAX_CHECKED_ACC:
                        checked = checked[-MAX_CHECKED_ACC:]
                    u["checked_acc"] = checked
                # Xóa acc đã check khỏi last_acc (giữ lại phần còn lại)
                if len(acc_list) > can_check:
                    u["last_acc"] = acc_list[can_check:]
                else:
                    u["last_acc"] = []
            if uid != ADMIN_ID:
                save_data()

            hit = [r for r in results if r["status"] == "HIT"]
            miss = [r for r in results if r["status"] != "HIT"]
            text = f"📊 *Kết quả check {len(results)} acc ({t1:.1f}s)*\n\n✅ HIT: {len(hit)}\n❌ MISS/ERROR: {len(miss)}\n"
            if hit:
                text += "\n*Chi tiết HIT:*\n"
                for r in hit[:10]:
                    ban_text = "Không"
                    if r.get("banned") == "YES":
                        ban_text = "Có"
                        if r.get("ban_start") and r.get("ban_end"):
                            ban_text += f" ({r['ban_start']} → {r['ban_end']})"
                        elif r.get("ban_start"):
                            ban_text += f" (từ {r['ban_start']})"
                    text += (f"👤 `{r['username']}` - {r.get('name','?')}\n"
                             f"   Rank: {r.get('rank','?')} | Lv: {r.get('level','?')}\n"
                             f"   Skin: {r.get('skins',0)} | Tướng: {r.get('champs',0)}\n"
                             f"   Ban: {ban_text}\n")
            if miss:
                text += "\n*MISS/ERROR:*\n"
                for r in miss[:5]: text += f"❌ `{r['username']}` - {r.get('message','?')}\n"
            if len(text) > 4000: text = text[:4000] + "\n... (cắt bớt)"
            kb, _ = main_menu(uid)
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        finally:
            with processing_lock:
                processing_users.discard(uid)

    elif data == "export":
        with users_lock:
            u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {"history":[]})
            history = list(u.get("history", []))
        if not history:
            await query.edit_message_text("📭 Chưa lấy acc nào.")
            return
        ts = int(time.time())
        fn = f"{OUTPUT_DIR}/acc_tho_{uid}_{ts}.txt"
        with open(fn, "w", encoding="utf-8") as f:
            f.write("\n".join(history))
        with open(fn, "rb") as f:
            await context.bot.send_document(chat_id=uid, document=f, filename=f"acc_tho_{ts}.txt",
                                            caption=f"📁 {len(history)} acc thô")
        await query.edit_message_text("✅ Đã gửi file acc thô!")

    elif data == "export_checked":
        with users_lock:
            u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {"checked_acc":[]})
            checked = list(u.get("checked_acc", []))
        if not checked:
            await query.edit_message_text("📭 Chưa check acc nào.")
            return
        ts = int(time.time())
        fn = f"{OUTPUT_DIR}/acc_check_{uid}_{ts}.txt"
        with open(fn, "w", encoding="utf-8") as f:
            f.write("\n".join(checked))
        with open(fn, "rb") as f:
            await context.bot.send_document(chat_id=uid, document=f, filename=f"acc_check_{ts}.txt",
                                            caption=f"📁 {len(checked)} acc đã check")
        await query.edit_message_text("✅ Đã gửi file acc check!")

    elif data == "profile":
        with users_lock:
            u = _get_user_copy(uid)
            if uid == ADMIN_ID:
                limit = 99999
                check_rem = 99999
            else:
                limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL
                today = today_vn()
                if u.get("last_check_date") != today:
                    check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
                else:
                    check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today", 0))
            vip_text = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
            total_acc = len(u.get("last_acc", []))
            checked_count = len(u.get("checked_acc", []))
        await query.edit_message_text(
            f"👤 {vip_text}\n📊 Lấy: {u.get('daily_used',0)}/{limit}\n"
            f"📦 Tổng acc đã lấy: {u.get('used',0)}\n"
            f"🔍 Check còn: {check_rem} lượt\n"
            f"📋 Acc chờ check: {total_acc}\n"
            f"✅ Đã check: {checked_count} acc"
        )

    elif data == "key_input":
        with users_lock:
            auth = (uid == ADMIN_ID) or (str(uid) in users and users[str(uid)].get("key") is not None)
        if auth:
            await query.answer("Bạn đã có key rồi!", show_alert=True)
        else:
            await query.edit_message_text("📝 Gửi key của bạn: /key <mã>")

async def key_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args:
        await update.message.reply_text("🔐 Key không đúng!")
        return
    key = context.args[0].strip().upper()
    expired = False
    is_vip = False
    with keys_lock:
        if key not in keys:
            await update.message.reply_text("🔐 Key không đúng hoặc đã hết hạn (key chỉ tồn tại 24h).")
            return
        kd = keys[key]
        if kd.get("banned"):
            await update.message.reply_text("❌ Key bị khóa")
            return
        if time.time() - kd.get("created_ts", 0) > KEY_EXPIRE_SECONDS:
            del keys[key]
            expired = True
        else:
            is_vip = kd.get("vip", False)
            del keys[key]
    if expired:
        save_data()
        await update.message.reply_text("❌ Key đã hết hạn (24h). Vui lòng lấy key mới.")
        return
    with users_lock:
        users[uid] = {
            "key": key, "vip": is_vip, "used": 0, "daily_used": 0,
            "date": today_vn(), "banned": False, "history": [],
            "last_check_date": today_vn(), "checked_today": 0,
            "name": update.effective_user.full_name or uid,
            "last_acc": [], "checked_acc": []
        }
    save_data()
    limit = LIMIT_VIP if is_vip else LIMIT_NORMAL
    kb, text = main_menu(int(uid))
    await update.message.reply_text(
        f"✅ {'👑 VIP' if is_vip else '🔑 Thường'} | {limit} acc/ngày\n"
        f"Check: {CHECK_LIMIT_VIP if is_vip else CHECK_LIMIT_NORMAL} acc/ngày\n"
        "⏳ Key tồn tại 24h, sau 24h sẽ tự mất.",
        reply_markup=kb, parse_mode=ParseMode.MARKDOWN
    )

async def lay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid) and not (str(uid) in users and users.get(str(uid), {}).get("key")):
        await update.message.reply_text("🔐 Chưa nhập key.")
        return
    if check_spam(uid):
        await update.message.reply_text("🚫 Spam!")
        return
    with users_lock:
        u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {})
        if u.get("banned"):
            await update.message.reply_text("🚫 Bị khóa")
            return
        if uid != ADMIN_ID:
            if u.get("date") != today_vn():
                u["date"] = today_vn()
                u["daily_used"] = 0
            limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL
            remaining = limit - u.get("daily_used", 0)
            try:
                n = int(context.args[0]) if context.args else 10
            except:
                n = 10
            if n > remaining:
                await update.message.reply_text(f"⚠️ Còn {remaining} acc")
                return
        else:
            try:
                n = int(context.args[0]) if context.args else 10
            except:
                n = 10
        if n > MAX_PER_REQ and uid != ADMIN_ID:
            await update.message.reply_text(f"Tối đa {MAX_PER_REQ} acc/lần.")
            n = MAX_PER_REQ

    with processing_lock:
        if uid in processing_users:
            await update.message.reply_text("⏳ Đợi...")
            return
        processing_users.add(uid)
    try:
        msg = await update.message.reply_text(f"⏳ {n} acc...")
        loop = asyncio.get_event_loop()
        accs = await loop.run_in_executor(None, fetch_fast, n)
        if not accs:
            await msg.edit_text("Không lấy được")
            return
        with users_lock:
            u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {})
            hist = u.get("history", [])
            hist = (hist + accs)[-MAX_HISTORY:]
            u["history"] = hist
            u["used"] = u.get("used", 0) + len(accs)
            u["daily_used"] = u.get("daily_used", 0) + len(accs)
            u["last_acc"] = accs.copy()
            last_copy = list(accs)
            total_acc = len(accs)
        if uid != ADMIN_ID:
            save_data()
        with users_lock:
            if uid == ADMIN_ID:
                check_rem = 99999
            else:
                u = users[str(uid)]
                today = today_vn()
                if u.get("last_check_date") != today:
                    check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
                else:
                    check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today", 0))
        check_buttons = [InlineKeyboardButton(f"✅ Check {x}", callback_data=f"checkacc_{x}")
                         for x in [5,10,15,20,25] if x <= check_rem and x <= total_acc]
        acc_text = "\n".join(last_copy[:20])
        if total_acc > 20:
            acc_text += f"\n... và {total_acc-20} acc khác"
        text = f"🎉 {len(accs)} acc\n\n{acc_text}"
        reply_markup = InlineKeyboardMarkup([check_buttons[i:i+3] for i in range(0, len(check_buttons), 3)]) if check_buttons else None
        await msg.edit_text(text, reply_markup=reply_markup)
    finally:
        with processing_lock:
            processing_users.discard(uid)

# ======================== ADMIN HANDLERS ========================
async def genkey(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 20: n = 20
    with keys_lock:
        _clean_expired_keys_nolock()
        new = []
        for _ in range(n):
            k = secrets.token_hex(8).upper()
            keys[k] = {"type":"normal","created":datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),"created_ts":time.time()}
            new.append(k)
    save_data()
    await update.message.reply_text(f"🔑 Key thường ({LIMIT_NORMAL}/ngày) - 24h:\n" + "\n".join(f"`{k}`" for k in new), parse_mode=ParseMode.MARKDOWN)

async def genvip(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 10: n = 10
    with keys_lock:
        _clean_expired_keys_nolock()
        new = []
        for _ in range(n):
            k = secrets.token_hex(8).upper()
            keys[k] = {"type":"vip","vip":True,"created":datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),"created_ts":time.time()}
            new.append(k)
    save_data()
    await update.message.reply_text(f"👑 Key VIP ({LIMIT_VIP}/ngày) - 24h:\n" + "\n".join(f"`{k}`" for k in new), parse_mode=ParseMode.MARKDOWN)

async def key_status(update, context):
    if not is_admin(update.effective_user.id): return
    with keys_lock:
        k_list = list(keys.items())[:15]
        k_text = "\n".join(f"`{k}` ({'VIP' if v.get('vip') else 'TH'})" for k,v in k_list) if k_list else ""
    with users_lock:
        u_list = list(users.items())[:20]
        u_text = ""
        for uid, u in u_list:
            if uid in ("admin", str(ADMIN_ID)): continue
            limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL
            u_text += f"👤 {u.get('name',uid)} | `{uid}` | {u.get('key','?')[:8]}... ({'VIP' if u.get('vip') else 'TH'}) | {u.get('daily_used',0)}/{limit}\n"
    t = "📊 *Trạng thái*\n\n"
    if k_text: t += "🔑 *Chưa dùng:*\n" + k_text
    if u_text: t += "\n👥 *Đã dùng:*\n" + u_text
    if not k_text and not u_text: t += "📭 Trống"
    await update.message.reply_text(t, parse_mode=ParseMode.MARKDOWN)

async def users_list(update, context):
    if not is_admin(update.effective_user.id): return
    with users_lock:
        if not users: await update.message.reply_text("📭 Trống"); return
        u_list = [f"👤 {u.get('name',uid)} | `{uid}`" for uid,u in users.items() if uid not in ("admin",str(ADMIN_ID))][:30]
    await update.message.reply_text("👥 *Users:*\n" + "\n".join(u_list), parse_mode=ParseMode.MARKDOWN)

async def stats(update, context):
    if not is_admin(update.effective_user.id): return
    with keys_lock:
        nk = sum(1 for v in keys.values() if not v.get("vip"))
        vk = sum(1 for v in keys.values() if v.get("vip"))
    with users_lock:
        total_used = sum(u.get("used",0) for u in users.values())
        user_count = len(users)
    with acc_lock:
        acc_count = len(used_accounts)
    with spam_lock:
        muted_count = len(muted_users)
    await update.message.reply_text(f"📊 Key: {nk}+{vk}VIP | 👥 {user_count} | 📦 {total_used} | 📋 {acc_count} | 🤐 {muted_count}")

async def listkeys(update, context):
    if not is_admin(update.effective_user.id): return
    with keys_lock:
        if not keys: await update.message.reply_text("📭 Trống"); return
        k_list = [f"`{k}` ({'VIP' if v.get('vip') else 'TH'})" for k,v in list(keys.items())[:20]]
    await update.message.reply_text("🔑 *Kho key:*\n" + "\n".join(k_list), parse_mode=ParseMode.MARKDOWN)

async def ban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    with users_lock:
        if uid in users:
            users[uid]["banned"] = True
        else:
            await update.message.reply_text("❌ Không tìm thấy")
            return
    save_data()
    await update.message.reply_text(f"✅ Ban {uid}")

async def unban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    with users_lock:
        if uid in users:
            users[uid]["banned"] = False
        else:
            await update.message.reply_text("❌ Không tìm thấy")
            return
    save_data()
    await update.message.reply_text(f"✅ Unban {uid}")

async def revoke(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    with users_lock:
        if uid in users:
            del users[uid]
        else:
            await update.message.reply_text("❌ Không tìm thấy")
            return
    save_data()
    await update.message.reply_text(f"✅ Thu hồi {uid}")

async def reset(update, context):
    if not is_admin(update.effective_user.id): return
    if context.args:
        uid = context.args[0]
        with users_lock:
            if uid in users:
                users[uid]["daily_used"] = 0
            else:
                await update.message.reply_text("❌ Không tìm thấy")
                return
    else:
        with users_lock:
            for u in users.values():
                u["daily_used"] = 0
    save_data()
    await update.message.reply_text("✅ Reset hết")

async def delkey(update, context):
    if not is_admin(update.effective_user.id): return
    key = context.args[0].strip().upper() if context.args else ""
    with keys_lock:
        if key in keys:
            del keys[key]
        else:
            await update.message.reply_text("❌ Không tìm thấy")
            return
    save_data()
    await update.message.reply_text("✅ Xóa key")

async def muted_list(update, context):
    if not is_admin(update.effective_user.id): return
    with spam_lock:
        if not muted_users:
            await update.message.reply_text("📭 Không ai mute")
            return
        now = time.time()
        lines = []
        for uid, until in muted_users.items():
            if until > now:
                with users_lock:
                    u = users.get(str(uid), {})
                    name = u.get("name", str(uid))
                lines.append(f"👤 {name} | `{uid}` | ⏳{int(until-now)}s")
        t = "\n".join(lines[:20])
    await update.message.reply_text(f"🤐 *Mute:*\n\n{t or '📭'}", parse_mode=ParseMode.MARKDOWN)

async def unmute_cmd(update, context):
    if not is_admin(update.effective_user.id): return
    uid = int(context.args[0]) if context.args else 0
    with spam_lock:
        if uid in muted_users:
            del muted_users[uid]
            if uid in request_log:
                del request_log[uid]
            await update.message.reply_text(f"✅ Unmute {uid}")
        else:
            await update.message.reply_text("❌ Không bị mute")

async def resetall(update, context):
    if not is_admin(update.effective_user.id): return
    if not context.args or context.args[0] != "CONFIRM":
        await update.message.reply_text("⚠️ /resetall CONFIRM")
        return
    with keys_lock: keys.clear()
    with users_lock: users.clear()
    with acc_lock: used_accounts.clear()
    with spam_lock: request_log.clear(); muted_users.clear()
    save_data()
    await update.message.reply_text("☢️ Xóa toàn bộ!")

async def error_handler(update, context):
    log.exception(context.error)

# ======================== MAIN ========================
if __name__ == "__main__":
    Thread(target=web, daemon=True).start()
    log.info(f"🤖 LQ ACC BOT | Admin:{ADMIN_ID}")
    load_data()
    application = Application.builder().token(BOT_TOKEN).build()
    handlers = [
        ("start", start), ("key", key_cmd), ("lay", lay),
        ("genkey", genkey), ("genvip", genvip), ("status", key_status), ("users", users_list),
        ("stats", stats), ("keys", listkeys), ("ban", ban), ("unban", unban), ("revoke", revoke),
        ("reset", reset), ("delkey", delkey), ("muted", muted_list), ("unmute", unmute_cmd),
        ("resetall", resetall)
    ]
    for cmd, h in handlers:
        application.add_handler(CommandHandler(cmd, h))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_error_handler(error_handler)
    application.run_polling(drop_pending_updates=True)
