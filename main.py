import asyncio, aiohttp, os, time, logging, secrets, pytz, json
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot đang chạy!"

def web():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

Thread(target=web, daemon=True).start()

# ===== CONFIG =====
BOT_TOKEN = "8258187122:AAF0qdhmiuX29smYszqFIgrs_iVgWF_rpUo"
ADMIN_ID = 8721023843
TANGACC_TOKEN = "https://tangacc.net/token.php"
TANGACC_ACC   = "https://tangacc.net/get_lq_acc.php"
THREADS = 50                     # Số thread lấy acc đồng thời
TIMEOUT = 10
OUTPUT_DIR = "lq_data"
JSONBIN_API_KEY = "$2a$10$ZKItx9kCcaQktuLuBDKY1ewYhT2gy3OWH.w7nkeTLWUy9sCxtjVWO"
JSONBIN_BIN_ID = "6a6b41e8da38895dfea40e00"
JSONBIN_API_URL = "https://api.jsonbin.io/v3/b"
LIMIT_NORMAL = 100
LIMIT_VIP = 550
MAX_PER_REQ = 50
KEY_EXPIRE_DAYS = 30
TZ = pytz.timezone("Asia/Ho_Chi_Minh")
SPAM_WINDOW = 10
MAX_SPAM = 5
MUTE_TIME = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
log = logging.getLogger(__name__)
file_lock = threading.Lock()
keys = {}
users = {}
used_accounts = set()

HEADERS = {
    "authority": "tangacc.net",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://tangacc.net/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# ===== JSONBIN =====
def load_json(fn):
    if os.path.exists(fn):
        with open(fn, "r", encoding="utf-8") as f: return json.load(f)
    return {}
def atomic_save(fn, data):
    tmp = fn + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, fn)

def load_data():
    global keys, users, used_accounts
    if JSONBIN_BIN_ID:
        try:
            r = requests.get(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}/latest", headers={"X-Master-Key": JSONBIN_API_KEY}, timeout=10)
            if r.status_code == 200:
                data = r.json().get("record", {})
                keys = data.get("keys", {})
                users = data.get("users", {})
                used_accounts = set(data.get("used_accounts", []))
                log.info("Đã tải dữ liệu từ bin.")
                return
        except: pass
    keys = {}
    users = {}
    used_accounts = set()

def save_data():
    data = {"keys": keys, "users": users, "used_accounts": list(used_accounts)}
    try:
        requests.put(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}", json=data, headers={"X-Master-Key": JSONBIN_API_KEY, "Content-Type": "application/json"}, timeout=10)
    except: pass

# ===== TANGACC ENGINE =====
def get_acc(session):
    try:
        r = session.get(TANGACC_TOKEN, headers=HEADERS, timeout=TIMEOUT)
        token = r.text.strip()
        if not token: return None
        h = {**HEADERS, "content-type": "application/x-www-form-urlencoded", "origin": "https://tangacc.net"}
        r2 = session.post(TANGACC_ACC, headers=h, data={"token": token}, timeout=TIMEOUT)
        acc = r2.text.strip()
        if acc and not acc.startswith("WAIT") and "|" in acc:
            return acc
    except: pass
    return None

def fetch_fast(n):
    """Lấy n acc – dùng ThreadPoolExecutor giống scan_fast.py"""
    acc_set = set()
    live_accs = []
    lock = threading.Lock()
    t0 = time.time()

    def worker():
        session = requests.Session()
        while len(live_accs) < n:
            acc = get_acc(session)
            if not acc: continue
            with lock:
                if acc in acc_set or acc in used_accounts: continue
                acc_set.add(acc)
                used_accounts.add(acc)
                live_accs.append(acc)
            if len(live_accs) >= n: break

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = [ex.submit(worker) for _ in range(THREADS)]
        while len(live_accs) < n:
            time.sleep(0.5)
        for f in futures: f.cancel()

    log.info(f"Lấy {len(live_accs)} acc trong {time.time()-t0:.1f}s")
    return live_accs[:n]

# ===== CÁC HÀM KHÁC =====
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
    keyboard = [
        [InlineKeyboardButton("🎮 Lấy 10 Acc", callback_data="lay_10"),
         InlineKeyboardButton("🎮 Lấy 30 Acc", callback_data="lay_30")],
        [InlineKeyboardButton("🎮 Lấy 50 Acc", callback_data="lay_50")],
        [InlineKeyboardButton("📁 Xuất File Lịch Sử", callback_data="export")],
        [InlineKeyboardButton("👤 Profile", callback_data="profile")],
        [InlineKeyboardButton("🔑 Nhập Key Mới", callback_data="key_input")],
    ]
    return InlineKeyboardMarkup(keyboard), f"🤖 *LQ ACC BOT*\n{vip}\n📊 Hôm nay: {used}/{limit} (Còn {remaining})\n⚡TangAcc\nChọn chức năng:"

def admin_menu_text():
    return (
        "👑 *ADMIN MENU*\n\n"
        "/genkey - Tạo key thường\n"
        "/genvip - Tạo key VIP\n"
        "/status - Trạng thái key & user\n"
        "/users - Danh sách người dùng\n"
        "/stats - Thống kê tổng\n"
        "/keys - Key chưa dùng\n"
        "/muted - Người đang bị mute\n"
        "/unmute <id> - Bỏ mute\n"
        "/ban <id> - Khóa người dùng\n"
        "/unban <id> - Mở khóa\n"
        "/revoke <id> - Thu hồi key\n"
        "/reset <id> - Reset lượt dùng\n"
        "/delkey <key> - Xóa key\n"
        "/resetall CONFIRM - Xóa toàn bộ"
    )

def login_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Nhập Key", callback_data="key_input")],
        [InlineKeyboardButton("🔗 Lấy Key Free", url="https://t.me/chantuiii")],
    ])

# ===== HANDLERS =====
async def start(update, context):
    uid = update.effective_user.id
    if uid != ADMIN_ID and check_spam(uid): await update.message.reply_text("🚫 Spam! Chờ 60s."); return
    user = update.effective_user; name = user.full_name or user.username or str(uid)
    if str(uid) in users: users[str(uid)]["name"] = name; save_data()
    if uid == ADMIN_ID: await update.message.reply_text(admin_menu_text(), parse_mode="Markdown")
    elif is_auth(uid):
        kb, text = main_menu(uid); await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.message.reply_text(
            "🤖 *LQ ACC BOT*\n\n🔐 Bạn chưa có key.\n👉 Nhấn nút dưới để lấy key free.",
            reply_markup=login_menu(), parse_mode="Markdown")

async def button_handler(update, context):
    query = update.callback_query; await query.answer()
    uid = query.from_user.id; data = query.data
    if not is_auth(uid) and data not in ["key_input"]:
        await query.edit_message_text("🔐 Chưa nhập key!", reply_markup=login_menu()); return
    if check_spam(uid): await query.edit_message_text("🚫 Spam!"); return
    if data.startswith("lay_"):
        n = int(data.split("_")[1]); u = get_user(uid); limit = get_limit(uid)
        if u.get("banned"): await query.edit_message_text("🚫 Đã bị khóa!"); return
        if u.get("date") != today_vn(): u["date"] = today_vn(); u["daily_used"] = 0
        if uid != ADMIN_ID:
            remaining = limit - u.get("daily_used", 0)
            if remaining <= 0: await query.edit_message_text(f"🚫 Hết {limit} acc/ngày"); return
            if n > remaining: n = remaining
        if n > MAX_PER_REQ: n = MAX_PER_REQ
        if uid in processing_users: await query.edit_message_text("⏳ Đợi..."); return
        processing_users.add(uid); await query.edit_message_text(f"⏳ Đang lấy {n} acc...")
        t0 = time.time(); accs = fetch_fast(n); t1 = time.time() - t0
        processing_users.discard(uid)
        if not accs: await query.edit_message_text("❌ Không lấy được acc."); return
        u["history"] = u.get("history",[]) + accs; u["used"] = u.get("used",0) + len(accs)
        u["daily_used"] = u.get("daily_used",0) + len(accs)
        if uid != ADMIN_ID: save_data()
        await query.edit_message_text(f"🎉 {len(accs)} acc ({t1:.1f}s)\n\n{chr(10).join(accs)[:3800]}")
        kb, text = main_menu(uid); await context.bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode="Markdown")
    elif data == "export":
        u = get_user(uid); history = u.get("history",[])
        if not history: await query.edit_message_text("📭 Chưa có acc."); return
        ts = int(time.time()); fn = f"{OUTPUT_DIR}/history_{uid}_{ts}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(history))
        with open(fn, "rb") as f: await context.bot.send_document(chat_id=uid, document=f, filename="lich_su_acc.txt", caption=f"📁 {len(history)} acc")
        await query.edit_message_text("✅ Đã gửi file!")
    elif data == "profile":
        u = get_user(uid); limit = get_limit(uid)
        vip = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
        await query.edit_message_text(f"👤 {vip}\n📊 {u.get('daily_used',0)}/{limit}\n📦 Tổng: {u.get('used',0)}")
    elif data == "key_input": await query.edit_message_text("📝 Gửi key: /key <mã>")

async def key_cmd(update, context):
    uid = str(update.effective_user.id)
    if not context.args: return await update.message.reply_text("🔐 Key không đúng!")
    key = context.args[0].strip().upper()
    if key in keys:
        kd = keys[key]
        if kd.get("banned"): return await update.message.reply_text("❌ Key bị khóa")
        if time.time() - kd.get("created_ts", 0) > KEY_EXPIRE_DAYS * 86400: return await update.message.reply_text("❌ Key hết hạn")
        is_vip = kd.get("vip", False)
        users[uid] = {"key":key,"vip":is_vip,"used":0,"daily_used":0,"date":today_vn(),"banned":False,"history":[],"name":update.effective_user.full_name or uid}
        del keys[key]; save_data()
        kb, text = main_menu(int(uid))
        await update.message.reply_text(f"✅ {'👑 VIP' if is_vip else '🔑 Thường'} | {LIMIT_VIP if is_vip else LIMIT_NORMAL} acc/ngày", reply_markup=kb, parse_mode="Markdown")
    else: await update.message.reply_text("🔐 Key không đúng!")

async def lay(update, context):
    uid = update.effective_user.id
    if not is_auth(uid): return await update.message.reply_text("🔐 Chưa nhập key.")
    if check_spam(uid): return await update.message.reply_text("🚫 Spam!")
    u = get_user(uid)
    if u.get("banned"): return await update.message.reply_text("🚫 Bị khóa")
    limit = get_limit(uid)
    if u.get("date") != today_vn(): u["date"] = today_vn(); u["daily_used"] = 0
    try: n = int(context.args[0]) if context.args else 10
    except: n = 10
    if uid != ADMIN_ID:
        remaining = limit - u.get("daily_used", 0)
        if n > remaining: await update.message.reply_text(f"⚠️ Còn {remaining} acc"); return
    processing_users.add(uid); msg = await update.message.reply_text(f"⏳ {n} acc...")
    accs = fetch_fast(n); processing_users.discard(uid)
    if not accs: return await msg.edit_text("Không lấy được")
    u["history"] = u.get("history",[]) + accs; u["used"] = u.get("used",0) + len(accs)
    u["daily_used"] = u.get("daily_used",0) + len(accs)
    if uid != ADMIN_ID: save_data()
    await msg.edit_text(f"🎉 {len(accs)} acc\n\n{chr(10).join(accs)[:3800]}")

async def export(update, context):
    uid = update.effective_user.id
    if not is_auth(uid): return await update.message.reply_text("🔐 Chưa nhập key.")
    u = get_user(uid); history = u.get("history",[])
    if not history: return await update.message.reply_text("📭 Chưa lấy acc")
    ts = int(time.time()); fn = f"{OUTPUT_DIR}/history_{uid}_{ts}.txt"
    with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(history))
    with open(fn, "rb") as f: await context.bot.send_document(chat_id=uid, document=f, filename="lich_su_acc.txt", caption=f"📁 {len(history)} acc")
    await update.message.reply_text("✅ Đã gửi!")

async def profile(update, context):
    uid = update.effective_user.id
    if not is_auth(uid): return await update.message.reply_text("🔐 Chưa nhập key.")
    u = get_user(uid); limit = get_limit(uid)
    vip = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
    await update.message.reply_text(f"👤 {vip}\n📊 {u.get('daily_used',0)}/{limit}\n📦 Tổng: {u.get('used',0)}")

async def getkey(update, context): await update.message.reply_text("🔑 Liên hệ t.me/chantuiii để lấy key free!")

async def genkey(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 20: n = 20
    new_keys = []
    for _ in range(n):
        k = secrets.token_hex(8).upper()
        keys[k] = {"type":"normal","created":datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),"created_ts":time.time()}
        new_keys.append(k)
    save_data()
    await update.message.reply_text(f"🔑 Key thường ({LIMIT_NORMAL} acc/ngày):\n" + "\n".join(f"`{k}`" for k in new_keys), parse_mode="Markdown")

async def genvip(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 10: n = 10
    new_keys = []
    for _ in range(n):
        k = secrets.token_hex(8).upper()
        keys[k] = {"type":"vip","vip":True,"created":datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),"created_ts":time.time()}
        new_keys.append(k)
    save_data()
    await update.message.reply_text(f"👑 Key VIP ({LIMIT_VIP} acc/ngày):\n" + "\n".join(f"`{k}`" for k in new_keys), parse_mode="Markdown")

async def key_status(update, context):
    if not is_admin(update.effective_user.id): return
    text = "📊 *Trạng thái*\n\n"
    if keys: text += "🔑 *Chưa dùng:*\n" + "\n".join(f"`{k}` ({'VIP' if v.get('vip') else 'TH'})" for k,v in list(keys.items())[:15])
    if users:
        text += "\n👥 *Đã dùng:*\n"
        for uid, u in list(users.items())[:20]:
            if uid in ("admin", str(ADMIN_ID)): continue
            text += f"👤 {u.get('name',uid)} | `{uid}` | {u.get('key','?')[:8]}... ({'VIP' if u.get('vip') else 'TH'}) | {u.get('daily_used',0)}/{get_limit(int(uid))}\n"
    await update.message.reply_text(text or "📭 Trống", parse_mode="Markdown")

async def users_list(update, context):
    if not is_admin(update.effective_user.id): return
    if not users: return await update.message.reply_text("📭 Chưa có user")
    await update.message.reply_text("👥 *Users:*\n" + "\n".join(f"👤 {u.get('name',uid)} | `{uid}` | {u.get('key','?')[:8]}..." for uid,u in list(users.items())[:30] if uid not in ("admin",str(ADMIN_ID))), parse_mode="Markdown")

async def stats(update, context):
    if not is_admin(update.effective_user.id): return
    nk = sum(1 for k in keys.values() if not k.get("vip")); vk = sum(1 for k in keys.values() if k.get("vip"))
    total = sum(u.get("used",0) for u in users.values())
    await update.message.reply_text(f"📊 Key: {nk}+{vk}VIP | 👥 {len(users)} | 📦 {total} acc | 📋 {len(used_accounts)} unique | 🤐 {len(muted_users)} muted")

async def listkeys(update, context):
    if not is_admin(update.effective_user.id): return
    if not keys: return await update.message.reply_text("📭 Kho key trống")
    await update.message.reply_text("🔑 *Kho key:*\n" + "\n".join(f"`{k}` ({'VIP' if v.get('vip') else 'TH'})" for k,v in list(keys.items())[:20]), parse_mode="Markdown")

async def ban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    if uid in users: users[uid]["banned"] = True; save_data(); await update.message.reply_text(f"✅ Đã ban {uid}")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def unban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    if uid in users: users[uid]["banned"] = False; save_data(); await update.message.reply_text(f"✅ Đã unban {uid}")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def revoke(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    if uid in users: del users[uid]; save_data(); await update.message.reply_text(f"✅ Đã thu hồi key của {uid}")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def reset(update, context):
    if not is_admin(update.effective_user.id): return
    if context.args:
        uid = context.args[0]
        if uid in users: users[uid]["daily_used"] = 0; save_data(); await update.message.reply_text(f"✅ Reset {uid}")
        else: await update.message.reply_text("❌ Không tìm thấy")
    else:
        for u in users: users[u]["daily_used"] = 0
        save_data(); await update.message.reply_text("✅ Reset tất cả")

async def delkey(update, context):
    if not is_admin(update.effective_user.id): return
    key = context.args[0].strip().upper() if context.args else ""
    if key in keys: del keys[key]; save_data(); await update.message.reply_text("✅ Đã xóa key")
    else: await update.message.reply_text("❌ Không tìm thấy")

async def muted_list(update, context):
    if not is_admin(update.effective_user.id): return
    if not muted_users: return await update.message.reply_text("📭 Không có ai bị mute")
    now = time.time()
    text = "\n".join(f"👤 {get_user(int(uid)).get('name',uid)} | `{uid}` | ⏳ {int(until-now)}s" for uid,until in list(muted_users.items())[:20] if until > now)
    await update.message.reply_text(f"🤐 *Mute:*\n\n{text or '📭 Trống'}", parse_mode="Markdown")

async def unmute_cmd(update, context):
    if not is_admin(update.effective_user.id): return
    uid = int(context.args[0]) if context.args else 0
    if uid in muted_users: del muted_users[uid]; request_log[uid].clear(); await update.message.reply_text(f"✅ Đã bỏ mute {uid}")
    else: await update.message.reply_text("❌ Không bị mute")

async def resetall(update, context):
    if not is_admin(update.effective_user.id): return
    if not context.args or context.args[0] != "CONFIRM": return await update.message.reply_text("⚠️ /resetall CONFIRM")
    keys.clear(); users.clear(); used_accounts.clear(); request_log.clear(); muted_users.clear()
    save_data(); await update.message.reply_text("☢️ Đã xóa toàn bộ!")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE): log.exception(context.error)

if __name__ == "__main__":
    log.info(f"🤖 LQ ACC BOT | Admin:{ADMIN_ID} | TangAcc {THREADS} threads")
    load_data()
    # Chạy proxy refresher nền nếu cần (bỏ qua – code gốc không cần)
    app_bot = Application.builder().token(BOT_TOKEN).build()
    for cmd, handler in [
        ("start", start), ("getkey", getkey), ("key", key_cmd), ("lay", lay),
        ("export", export), ("profile", profile), ("genkey", genkey), ("genvip", genvip),
        ("status", key_status), ("users", users_list), ("stats", stats), ("keys", listkeys),
        ("ban", ban), ("unban", unban), ("revoke", revoke), ("reset", reset),
        ("delkey", delkey), ("muted", muted_list), ("unmute", unmute_cmd), ("resetall", resetall)
    ]:
        app_bot.add_handler(CommandHandler(cmd, handler))
    app_bot.add_handler(CallbackQueryHandler(button_handler))
    app_bot.add_error_handler(error_handler)
    app_bot.run_polling(drop_pending_updates=True)
