import asyncio, os, time, logging, secrets, pytz, json, threading, requests
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

app = Flask(__name__)
@app.route("/")
def home(): return "Bot đang chạy!"
def web(): app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
Thread(target=web, daemon=True).start()

BOT_TOKEN = "8258187122:AAF0qdhmiuX29smYszqFIgrs_iVgWF_rpUo"
ADMIN_ID = 8721023843
TANGACC_TOKEN = "https://tangacc.net/token.php"
TANGACC_ACC = "https://tangacc.net/get_lq_acc.php"
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
KEY_EXPIRE_DAYS = 30
TZ = pytz.timezone("Asia/Ho_Chi_Minh")
SPAM_WINDOW = 10
MAX_SPAM = 5
MUTE_TIME = 60
OUTPUT_DIR = "lq_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
log = logging.getLogger(__name__)

keys = {}
users = {}
used_accounts = set()
last_acc_list = {}

HEADERS = {
    "authority": "tangacc.net", "accept": "*/*", "accept-language": "en-US,en;q=0.9",
    "referer": "https://tangacc.net/", "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def load_data():
    global keys, users, used_accounts
    try:
        r = requests.get(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}/latest",
                         headers={"X-Master-Key": JSONBIN_API_KEY}, timeout=10)
        if r.status_code == 200:
            data = r.json().get("record", {})
            keys = data.get("keys", {})
            users = data.get("users", {})
            used_accounts = set(data.get("used_accounts", []))
            log.info(f"Đã tải bin: {len(keys)} keys, {len(users)} users")
            return
    except Exception as e: log.error(f"Lỗi tải bin: {e}")
    keys, users, used_accounts = {}, {}, set()

def save_data():
    for _ in range(3):
        try:
            data = {"keys": keys, "users": users, "used_accounts": list(used_accounts)}
            r = requests.put(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}", json=data,
                             headers={"X-Master-Key": JSONBIN_API_KEY, "Content-Type": "application/json"}, timeout=10)
            if r.status_code == 200: return
        except: pass
        time.sleep(1)

def get_acc(session):
    try:
        r = session.get(TANGACC_TOKEN, headers=HEADERS, timeout=TIMEOUT)
        token = r.text.strip()
        if not token: return None
        h = {**HEADERS, "content-type": "application/x-www-form-urlencoded", "origin": "https://tangacc.net"}
        r2 = session.post(TANGACC_ACC, headers=h, data={"token": token}, timeout=TIMEOUT)
        acc = r2.text.strip()
        if acc and not acc.startswith("WAIT") and "|" in acc: return acc
    except: pass
    return None

def fetch_fast(n):
    acc_set = set()
    live_accs = []
    lock = threading.Lock()
    stop_flag = threading.Event()
    t0 = time.time()
    def worker():
        session = requests.Session()
        fail_count = 0
        while not stop_flag.is_set() and len(live_accs) < n:
            acc = get_acc(session)
            if not acc:
                fail_count += 1
                if fail_count > 15: break
                continue
            fail_count = 0
            with lock:
                if acc in acc_set or acc in used_accounts: continue
                acc_set.add(acc)
                used_accounts.add(acc)
                live_accs.append(acc)
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        for _ in range(THREADS): ex.submit(worker)
        deadline = time.time() + 45
        while len(live_accs) < n and time.time() < deadline: time.sleep(0.3)
        stop_flag.set()
    log.info(f"Lấy {len(live_accs)} acc trong {time.time()-t0:.1f}s")
    return live_accs[:n]

def check_acc_api(username, password):
    try:
        r = requests.post(CHECK_API_URL, data={"user": username, "pass": password, "apikey": CHECK_API_KEY}, timeout=45)
        d = r.json()
        if d.get("ok") and d["result"].get("status") == "HIT":
            info = d["result"]; skins = info.get("aov_skins", {})
            return {"status": "HIT", "username": username, "name": info.get("aov_name"), "uid": info.get("uid"),
                    "rank": info.get("aov_rank"), "level": info.get("aov_level"), "skins": skins.get("total_skins", 0),
                    "champs": skins.get("total_champs", 0), "banned": info.get("aov_banned"), "fb_linked": info.get("fb_linked"),
                    "mobile_bound": info.get("mobile_bound"), "email_verified": info.get("email_verified")}
        else: return {"status": "MISS", "username": username, "message": d.get("result", {}).get("detail", "Sai mật khẩu / không tồn tại")}
    except Exception as e: return {"status": "ERROR", "username": username, "message": str(e)}

def get_check_limit(uid):
    if uid == ADMIN_ID: return 99999
    return CHECK_LIMIT_VIP if users.get(str(uid), {}).get("vip") else CHECK_LIMIT_NORMAL

def get_remaining_checks(uid):
    if uid == ADMIN_ID: return 99999
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    user = users.get(str(uid), {})
    if user.get("last_check_date") != today: return get_check_limit(uid)
    return max(0, get_check_limit(uid) - user.get("checked_today", 0))

def update_check_count(uid, count):
    uid = str(uid)
    if uid not in users: return
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    user = users[uid]
    if user.get("last_check_date") != today:
        user["last_check_date"] = today; user["checked_today"] = count
    else: user["checked_today"] = user.get("checked_today", 0) + count
    save_data()

request_log = defaultdict(list)
muted_users = {}
processing_users = set()

def today_vn(): return datetime.now(TZ).strftime("%Y-%m-%d")
def is_admin(uid): return uid == ADMIN_ID
def is_auth(uid):
    if uid == ADMIN_ID: return True
    return str(uid) in users and users[str(uid)].get("key") is not None
def get_limit(uid):
    if uid == ADMIN_ID: return 99999
    return LIMIT_VIP if users.get(str(uid), {}).get("vip") else LIMIT_NORMAL
def get_user(uid):
    if uid == ADMIN_ID:
        if "admin" not in users: users["admin"] = {"history":[],"used":0,"daily_used":0,"banned":False,"vip":True}
        return users["admin"]
    return users.get(str(uid), {})

def check_spam(uid):
    if uid == ADMIN_ID: return False
    now = time.time()
    if uid in muted_users:
        if now < muted_users[uid]: return True
        else: del muted_users[uid]
    request_log[uid] = [t for t in request_log[uid] if now - t < SPAM_WINDOW]
    request_log[uid].append(now)
    if len(request_log[uid]) > MAX_SPAM:
        muted_users[uid] = now + MUTE_TIME
        return True
    return False

def main_menu(uid):
    u = get_user(uid); limit = get_limit(uid); used = u.get("daily_used", 0)
    remaining = limit - used
    vip = "👑 VIP" if u.get("vip") else "🔑 Thường"
    check_rem = get_remaining_checks(uid)
    keyboard = [
        [InlineKeyboardButton("🎮 Lấy 10 Acc", callback_data="lay_10"), InlineKeyboardButton("🎮 Lấy 30 Acc", callback_data="lay_30")],
        [InlineKeyboardButton("🎮 Lấy 50 Acc", callback_data="lay_50")],
        [InlineKeyboardButton("📁 Xuất File", callback_data="export"), InlineKeyboardButton("👤 Profile", callback_data="profile")],
    ]
    text = f"🤖 *LQ ACC BOT*\n{vip}\n📊 Hôm nay: {used}/{limit} (Còn {remaining})\n🔍 Check: {check_rem} lượt\n⚡TangAcc\nChọn chức năng:"
    return InlineKeyboardMarkup(keyboard), text

def admin_menu_text():
    return ("👑 *ADMIN MENU*\n\n"
            "/genkey - Tạo key thường\n"
            "/genvip - Tạo key VIP\n"
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
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != ADMIN_ID and check_spam(uid): await update.message.reply_text("🚫 Spam! 60s."); return
    user = update.effective_user; name = user.full_name or user.username or str(uid)
    if str(uid) in users: users[str(uid)]["name"] = name; save_data()
    if uid == ADMIN_ID: await update.message.reply_text(admin_menu_text(), parse_mode=ParseMode.MARKDOWN)
    elif is_auth(uid):
        kb, text = main_menu(uid); await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else: await update.message.reply_text("🤖 *LQ ACC BOT*\n\n🔐 Chưa có key.\n👉 Nhấn nút dưới.", reply_markup=login_menu(), parse_mode=ParseMode.MARKDOWN)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    uid = query.from_user.id; data = query.data
    if not is_auth(uid) and data not in ["key_input"]:
        await query.edit_message_text("🔐 Bạn chưa nhập key!", reply_markup=login_menu()); return
    if check_spam(uid): await query.edit_message_text("🚫 Spam!"); return

    if data.startswith("lay_"):
        n = int(data.split("_")[1])
        u = get_user(uid); limit = get_limit(uid)
        if u.get("banned"): await query.edit_message_text("🚫 Đã bị khóa!"); return
        if u.get("date") != today_vn(): u["date"] = today_vn(); u["daily_used"] = 0
        if uid != ADMIN_ID:
            remaining = limit - u.get("daily_used", 0)
            if remaining <= 0: await query.edit_message_text(f"🚫 Hết {limit} acc/ngày"); return
            if n > remaining: n = remaining
        if n > MAX_PER_REQ: n = MAX_PER_REQ
        if uid in processing_users: await query.edit_message_text("⏳ Đợi..."); return
        processing_users.add(uid); await query.edit_message_text(f"⏳ Đang lấy {n} acc...")
        t0 = time.time()
        loop = asyncio.get_event_loop()
        accs = await loop.run_in_executor(None, fetch_fast, n)
        t1 = time.time() - t0
        processing_users.discard(uid)
        if not accs: await query.edit_message_text("❌ Không lấy được acc."); return
        u["history"] = u.get("history",[]) + accs; u["used"] = u.get("used",0) + len(accs)
        u["daily_used"] = u.get("daily_used",0) + len(accs)
        if uid != ADMIN_ID: save_data()
        # LƯU ACC ĐỂ CHECK
        last_acc_list[uid] = accs.copy()
        # Nút check
        check_rem = get_remaining_checks(uid)
        check_buttons = []
        for x in [5,10,15,20,25]:
            if x <= check_rem and x <= len(accs):
                check_buttons.append(InlineKeyboardButton(f"✅ Check {x} acc", callback_data=f"checkacc_{x}"))
        acc_text = "\n".join(accs[:20])
        if len(accs) > 20: acc_text += f"\n... và {len(accs)-20} acc khác"
        msg = f"🎉 {len(accs)} acc ({t1:.1f}s)\n\n{acc_text}"
        reply_markup = InlineKeyboardMarkup([check_buttons[i:i+3] for i in range(0, len(check_buttons), 3)]) if check_buttons else None
        await query.edit_message_text(msg, reply_markup=reply_markup)
        kb, text = main_menu(uid); await context.bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

    elif data.startswith("checkacc_"):
        parts = data.split("_")
        if len(parts) != 2: return
        req_count = int(parts[1])
        uid = query.from_user.id
        # KIỂM TRA CÓ ACC ĐÃ LẤY KHÔNG
        if uid not in last_acc_list or not last_acc_list[uid]:
            await query.edit_message_text("⚠️ Vui lòng lấy acc trước để sử dụng tính năng check.\n\nBạn cần lấy acc từ bot trước, sau đó nhấn nút Check ở danh sách acc vừa lấy.")
            return
        acc_list = last_acc_list[uid]
        remaining = get_remaining_checks(uid)
        can_check = min(req_count, len(acc_list), remaining)
        if can_check <= 0:
            await query.edit_message_text("🚫 Bạn đã hết lượt check hôm nay hoặc không có acc để check.")
            return
        # TIẾN HÀNH CHECK
        await query.edit_message_text(f"🔍 Đang check {can_check} acc, vui lòng đợi...")
        accs_to_check = acc_list[:can_check]
        results = []
        for acc_str in accs_to_check:
            if "|" not in acc_str: continue
            user, pw = acc_str.split("|", 1)
            res = check_acc_api(user, pw)
            results.append(res)
        update_check_count(uid, len(accs_to_check))
        hit = [r for r in results if r["status"] == "HIT"]
        miss = [r for r in results if r["status"] != "HIT"]
        text = f"📊 *Kết quả check {len(results)} acc*\n\n✅ HIT: {len(hit)}\n❌ MISS/ERROR: {len(miss)}\n"
        if hit:
            text += "\n*Chi tiết HIT:*\n"
            for r in hit[:5]:
                text += f"👤 `{r['username']}` - {r.get('name','?')}\n   Rank: {r.get('rank','?')} | Lv: {r.get('level','?')}\n   Skin: {r.get('skins',0)} | Tướng: {r.get('champs',0)}\n"
        if miss:
            text += "\n*MISS:*\n"
            for r in miss[:3]: text += f"❌ `{r['username']}` - {r.get('message','?')}\n"
        kb, _ = main_menu(uid)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    elif data == "export":
        u = get_user(uid); history = u.get("history",[])
        if not history: await query.edit_message_text("📭 Chưa lấy acc nào."); return
        ts = int(time.time()); fn = f"{OUTPUT_DIR}/history_{uid}_{ts}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(history))
        with open(fn, "rb") as f: await context.bot.send_document(chat_id=uid, document=f, filename=f"lich_su_acc_{ts}.txt", caption=f"📁 {len(history)} acc")
        await query.edit_message_text("✅ Đã gửi file!")

    elif data == "profile":
        u = get_user(uid); limit = get_limit(uid)
        vip = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
        check_rem = get_remaining_checks(uid)
        await query.edit_message_text(f"👤 {vip}\n📊 Lấy: {u.get('daily_used',0)}/{limit}\n📦 Tổng: {u.get('used',0)}\n🔍 Check: {check_rem} lượt\n📋 Lịch sử: {len(u.get('history',[]))} acc")

    elif data == "key_input":
        if is_auth(uid):
            await query.answer("Bạn đã có key rồi!", show_alert=True)
        else:
            await query.edit_message_text("📝 Gửi key của bạn: /key <mã>")

async def key_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args: await update.message.reply_text("🔐 Key không đúng!"); return
    key = context.args[0].strip().upper()
    if key not in keys: await update.message.reply_text("🔐 Key không đúng!"); return
    kd = keys[key]
    if kd.get("banned"): await update.message.reply_text("❌ Key bị khóa"); return
    if time.time() - kd.get("created_ts",0) > KEY_EXPIRE_DAYS*86400: await update.message.reply_text("❌ Key hết hạn"); return
    is_vip = kd.get("vip", False)
    users[uid] = {"key":key,"vip":is_vip,"used":0,"daily_used":0,"date":today_vn(),"banned":False,"history":[],
                  "last_check_date":today_vn(),"checked_today":0,"name":update.effective_user.full_name or uid}
    del keys[key]; save_data()
    limit = LIMIT_VIP if is_vip else LIMIT_NORMAL
    kb, text = main_menu(int(uid))
    await update.message.reply_text(f"✅ {'👑 VIP' if is_vip else '🔑 Thường'} | {limit} acc/ngày\nCheck: {CHECK_LIMIT_VIP if is_vip else CHECK_LIMIT_NORMAL} acc/ngày", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

async def lay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_auth(uid): await update.message.reply_text("🔐 Chưa nhập key."); return
    if check_spam(uid): await update.message.reply_text("🚫 Spam!"); return
    u = get_user(uid)
    if u.get("banned"): await update.message.reply_text("🚫 Bị khóa"); return
    limit = get_limit(uid)
    if u.get("date") != today_vn(): u["date"] = today_vn(); u["daily_used"] = 0
    try: n = int(context.args[0]) if context.args else 10
    except: n = 10
    if uid != ADMIN_ID:
        remaining = limit - u.get("daily_used", 0)
        if n > remaining: await update.message.reply_text(f"⚠️ Còn {remaining} acc"); return
    processing_users.add(uid); msg = await update.message.reply_text(f"⏳ {n} acc...")
    loop = asyncio.get_event_loop(); accs = await loop.run_in_executor(None, fetch_fast, n)
    processing_users.discard(uid)
    if not accs: await msg.edit_text("Không lấy được"); return
    u["history"] = u.get("history",[]) + accs; u["used"] = u.get("used",0) + len(accs)
    u["daily_used"] = u.get("daily_used",0) + len(accs)
    if uid != ADMIN_ID: save_data()
    last_acc_list[uid] = accs.copy()
    check_rem = get_remaining_checks(uid)
    check_buttons = [InlineKeyboardButton(f"✅ Check {x}", callback_data=f"checkacc_{x}") for x in [5,10,15,20,25] if x <= check_rem and x <= len(accs)]
    acc_text = "\n".join(accs[:20])
    if len(accs) > 20: acc_text += f"\n... và {len(accs)-20} acc khác"
    text = f"🎉 {len(accs)} acc\n\n{acc_text}"
    reply_markup = InlineKeyboardMarkup([check_buttons[i:i+3] for i in range(0, len(check_buttons), 3)]) if check_buttons else None
    await msg.edit_text(text, reply_markup=reply_markup)

# Admin handlers (giữ nguyên)
async def genkey(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 20: n = 20
    new = [secrets.token_hex(8).upper() for _ in range(n)]
    for k in new: keys[k] = {"type":"normal","created":datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),"created_ts":time.time()}
    save_data()
    await update.message.reply_text(f"🔑 Key thường ({LIMIT_NORMAL}/ngày):\n" + "\n".join(f"`{k}`" for k in new), parse_mode=ParseMode.MARKDOWN)

async def genvip(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 10: n = 10
    new = [secrets.token_hex(8).upper() for _ in range(n)]
    for k in new: keys[k] = {"type":"vip","vip":True,"created":datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),"created_ts":time.time()}
    save_data()
    await update.message.reply_text(f"👑 Key VIP ({LIMIT_VIP}/ngày):\n" + "\n".join(f"`{k}`" for k in new), parse_mode=ParseMode.MARKDOWN)

async def key_status(update, context):
    if not is_admin(update.effective_user.id): return
    t = "📊 *Trạng thái*\n\n"
    if keys: t += "🔑 *Chưa dùng:*\n" + "\n".join(f"`{k}` ({'VIP' if v.get('vip') else 'TH'})" for k,v in list(keys.items())[:15])
    if users:
        t += "\n👥 *Đã dùng:*\n"
        for uid,u in list(users.items())[:20]:
            if uid in ("admin",str(ADMIN_ID)): continue
            t += f"👤 {u.get('name',uid)} | `{uid}` | {u.get('key','?')[:8]}... ({'VIP' if u.get('vip') else 'TH'}) | {u.get('daily_used',0)}/{get_limit(int(uid))}\n"
    await update.message.reply_text(t or "📭 Trống", parse_mode=ParseMode.MARKDOWN)

async def users_list(update, context):
    if not is_admin(update.effective_user.id): return
    if not users: await update.message.reply_text("📭 Trống"); return
    await update.message.reply_text("👥 *Users:*\n" + "\n".join(f"👤 {u.get('name',uid)} | `{uid}`" for uid,u in list(users.items())[:30] if uid not in ("admin",str(ADMIN_ID))), parse_mode=ParseMode.MARKDOWN)

async def stats(update, context):
    if not is_admin(update.effective_user.id): return
    nk = sum(1 for v in keys.values() if not v.get("vip")); vk = sum(1 for v in keys.values() if v.get("vip"))
    total = sum(u.get("used",0) for u in users.values())
    await update.message.reply_text(f"📊 Key: {nk}+{vk}VIP | 👥 {len(users)} | 📦 {total} | 📋 {len(used_accounts)} | 🤐 {len(muted_users)}")

async def listkeys(update, context):
    if not is_admin(update.effective_user.id): return
    if not keys: await update.message.reply_text("📭 Trống"); return
    await update.message.reply_text("🔑 *Kho key:*\n" + "\n".join(f"`{k}` ({'VIP' if v.get('vip') else 'TH'})" for k,v in list(keys.items())[:20]), parse_mode=ParseMode.MARKDOWN)

async def ban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    if uid in users: users[uid]["banned"]=True; save_data(); await update.message.reply_text(f"✅ Ban {uid}")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def unban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    if uid in users: users[uid]["banned"]=False; save_data(); await update.message.reply_text(f"✅ Unban {uid}")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def revoke(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    if uid in users: del users[uid]; save_data(); await update.message.reply_text(f"✅ Thu hồi {uid}")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def reset(update, context):
    if not is_admin(update.effective_user.id): return
    if context.args:
        uid=context.args[0]
        if uid in users: users[uid]["daily_used"]=0; save_data(); await update.message.reply_text(f"✅ Reset {uid}")
        else: await update.message.reply_text("❌ Không tìm thấy")
    else:
        for u in users: users[u]["daily_used"]=0
        save_data(); await update.message.reply_text("✅ Reset hết")

async def delkey(update, context):
    if not is_admin(update.effective_user.id): return
    key = context.args[0].strip().upper() if context.args else ""
    if key in keys: del keys[key]; save_data(); await update.message.reply_text("✅ Xóa key")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def muted_list(update, context):
    if not is_admin(update.effective_user.id): return
    if not muted_users: await update.message.reply_text("📭 Không ai mute"); return
    now=time.time()
    t = "\n".join(f"👤 {get_user(int(uid)).get('name',uid)} | `{uid}` | ⏳{int(until-now)}s" for uid,until in list(muted_users.items())[:20] if until>now)
    await update.message.reply_text(f"🤐 *Mute:*\n\n{t or '📭'}", parse_mode=ParseMode.MARKDOWN)

async def unmute_cmd(update, context):
    if not is_admin(update.effective_user.id): return
    uid = int(context.args[0]) if context.args else 0
    if uid in muted_users: del muted_users[uid]; request_log[uid].clear(); await update.message.reply_text(f"✅ Unmute {uid}")
    else: await update.message.reply_text("❌ Không bị mute")

async def resetall(update, context):
    if not is_admin(update.effective_user.id): return
    if not context.args or context.args[0]!="CONFIRM": await update.message.reply_text("⚠️ /resetall CONFIRM"); return
    keys.clear(); users.clear(); used_accounts.clear(); request_log.clear(); muted_users.clear()
    save_data(); await update.message.reply_text("☢️ Xóa toàn bộ!")

async def error_handler(update, context): log.exception(context.error)

if __name__ == "__main__":
    log.info(f"🤖 LQ ACC BOT | Admin:{ADMIN_ID}")
    load_data()
    application = Application.builder().token(BOT_TOKEN).build()
    for cmd, h in [("start",start),("key",key_cmd),("lay",lay),
                   ("genkey",genkey),("genvip",genvip),("status",key_status),("users",users_list),
                   ("stats",stats),("keys",listkeys),("ban",ban),("unban",unban),("revoke",revoke),
                   ("reset",reset),("delkey",delkey),("muted",muted_list),("unmute",unmute_cmd),("resetall",resetall)]:
        application.add_handler(CommandHandler(cmd, h))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_error_handler(error_handler)
    application.run_polling(drop_pending_updates=True)
