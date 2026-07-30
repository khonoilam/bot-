import asyncio, aiohttp, os, time, logging, secrets, pytz, json
from datetime import datetime
from collections import defaultdict
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

BOT_TOKEN = "8258187122:AAF0qdhmiuX29smYszqFIgrs_iVgWF_rpUo"
ADMIN_ID = 8721023843
API_URL = "https://keyherlyswar.x10.mx/Apidocs/reg/reglq.php"
CONCURRENT = 20
OUTPUT_DIR = "lq_data"
JSONBIN_API_KEY = "$2a$10$ZKItx9kCcaQktuLuBDKY1ewYhT2gy3OWH.w7nkeTLWUy9sCxtjVWO"
JSONBIN_BIN_ID = "6a6b41e8d8a38895dfea4e00"
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
file_lock = asyncio.Lock()

keys = {}
users = {}
used_accounts = set()

async def load_data_from_bin():
    if not JSONBIN_BIN_ID:
        return None
    headers = {"X-Master-Key": JSONBIN_API_KEY}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}/latest", headers=headers, timeout=10) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("record", {})
                else:
                    log.error(f"Lỗi tải bin: {r.status}")
        except Exception as e:
            log.error(f"Lỗi kết nối jsonbin.io: {e}")
    return None

async def save_data_to_bin(data):
    headers = {"X-Master-Key": JSONBIN_API_KEY, "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.put(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}", json=data, headers=headers, timeout=10) as r:
                if r.status == 200:
                    log.info("Đã lưu dữ liệu lên jsonbin.io")
                else:
                    log.error(f"Lỗi lưu bin: {r.status}")
        except Exception as e:
            log.error(f"Lỗi kết nối khi lưu: {e}")

async def initialize_data():
    global keys, users, used_accounts
    if JSONBIN_BIN_ID:
        data = await load_data_from_bin()
        if data:
            keys = data.get("keys", {})
            users = data.get("users", {})
            used_accounts = set(data.get("used_accounts", []))
            log.info("Đã tải dữ liệu từ bin thành công.")
            return
    keys = {}
    users = {}
    used_accounts = set()
    log.warning("Không tải được dữ liệu từ bin, khởi tạo dữ liệu rỗng.")

async def safe_save(full=False):
    async with file_lock:
        data = {
            "keys": keys,
            "users": users,
            "used_accounts": list(used_accounts)
        }
        await save_data_to_bin(data)

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

async def get_acc(session, sem):
    async with sem:
        try:
            async with session.get(API_URL, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                                   timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    d = await r.json()
                    if d.get("status") and d.get("result"):
                        a = d["result"][0]
                        return f"{a['account']}|{a['password']}"
        except:
            pass
    return ""

async def fetch_fast(n):
    unique = []
    sem = asyncio.Semaphore(CONCURRENT)
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=conn) as s:
        while len(unique) < n:
            tasks = [get_acc(s, sem) for _ in range(CONCURRENT)]
            results = await asyncio.gather(*tasks)
            for r in results:
                if r and r not in used_accounts and r not in unique:
                    unique.append(r)
                    used_accounts.add(r)
                    if len(unique) >= n:
                        break
    await safe_save(full=True)
    return unique[:n]

def main_menu(uid):
    u = get_user(uid)
    limit = get_limit(uid)
    used = u.get("daily_used", 0)
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
    return InlineKeyboardMarkup(keyboard), f"🤖 *LQ ACC BOT*\n{vip}\n📊 Hôm nay: {used}/{limit} (Còn {remaining})\nChọn chức năng:"

def admin_menu_text():
    return "👑 *ADMIN*\n/genkey|/genvip|/status|/users|/stats|/keys\n/muted|/unmute|/ban|/unban|/revoke|/reset|/delkey|/resetall"

def login_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Nhập Key", callback_data="key_input")],
        [InlineKeyboardButton("🔗 Lấy Key Free", url="https://t.me/chantuiii")],
    ])

async def start(update, context):
    uid = update.effective_user.id
    if uid != ADMIN_ID and check_spam(uid):
        await update.message.reply_text("🚫 Bạn đã bị cấm chat 60 giây vì spam!")
        return
    user = update.effective_user
    name = user.full_name or user.username or str(uid)
    if str(uid) in users:
        users[str(uid)]["name"] = name
        await safe_save()
    if uid == ADMIN_ID:
        await update.message.reply_text(admin_menu_text(), parse_mode="Markdown")
    elif is_auth(uid):
        kb, text = main_menu(uid)
        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Nhấn vào đây để lấy Key Free", url="https://t.me/chantuiii")],
            [InlineKeyboardButton("🔑 Tôi đã có Key", callback_data="key_input")],
        ])
        await update.message.reply_text(
            "🤖 *LQ ACC BOT*\n\n"
            "🔐 Bạn chưa có key để sử dụng bot.\n\n"
            "👉 Nhấn nút bên dưới để *Lấy Key Free*\n"
            "   (ib t.me/chantuiii để nhận key)\n\n"
            "Nếu đã có key, nhấn *Tôi đã có Key* để nhập.",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )

async def button_handler(update, context):
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
        accs = await fetch_fast(n)
        t1 = time.time() - t0
        processing_users.discard(uid)
        if not accs:
            await query.edit_message_text("Không lấy được acc.")
            return
        u["history"] = u.get("history", []) + accs
        u["used"] = u.get("used", 0) + len(accs)
        u["daily_used"] = u.get("daily_used", 0) + len(accs)
        if uid != ADMIN_ID:
            await safe_save()
        await query.edit_message_text(f"🎉 {len(accs)} acc ({t1:.1f}s)\n\n{chr(10).join(accs)[:3800]}")
        kb, text = main_menu(uid)
        await context.bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode="Markdown")
    elif data == "export":
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
            await context.bot.send_document(chat_id=uid, document=f, filename="lich_su_acc.txt",
                                            caption=f"📁 {len(history)} acc")
        await query.edit_message_text("✅ Đã gửi file!")
    elif data == "profile":
        u = get_user(uid)
        limit = get_limit(uid)
        vip = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
        await query.edit_message_text(
            f"👤 {vip}\n📊 {u.get('daily_used', 0)}/{limit}\n📦 Tổng: {u.get('used', 0)}\n📋 Lịch sử: {len(u.get('history', []))} acc")
    elif data == "key_input":
        await query.edit_message_text("📝 Gửi key của bạn: /key <mã>")

async def key_cmd(update, context):
    uid = str(update.effective_user.id)
    if not context.args:
        return await update.message.reply_text("🔐 Key không đúng! Nhấn nút Lấy Key Free để được cấp key.")
    key = context.args[0].strip().upper()
    if key in keys:
        kd = keys[key]
        if kd.get("banned"):
            return await update.message.reply_text("❌ Key bị khóa")
        if time.time() - kd.get("created_ts", 0) > KEY_EXPIRE_DAYS * 86400:
            return await update.message.reply_text("❌ Key hết hạn")
        is_vip = kd.get("vip", False)
        users[uid] = {
            "key": key,
            "vip": is_vip,
            "used": 0,
            "daily_used": 0,
            "date": today_vn(),
            "banned": False,
            "history": [],
            "name": update.effective_user.full_name or uid
        }
        del keys[key]
        await safe_save()
        limit = LIMIT_VIP if is_vip else LIMIT_NORMAL
        kb, text = main_menu(int(uid))
        await update.message.reply_text(f"✅ {'👑 VIP' if is_vip else '🔑 Thường'} | {limit} acc/ngày",
                                        reply_markup=kb, parse_mode="Markdown")
    else:
        await update.message.reply_text("🔐 Key không đúng! Nhấn nút Lấy Key Free để được cấp key.")

async def lay(update, context):
    uid = update.effective_user.id
    if not is_auth(uid):
        return await update.message.reply_text("🔐 Chưa nhập key\nNhấn nút Lấy Key Free để được cấp key.")
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
    except:
        n = 10
    if uid != ADMIN_ID:
        remaining = limit - u.get("daily_used", 0)
        if n > MAX_PER_REQ:
            n = MAX_PER_REQ
        if n > remaining:
            await update.message.reply_text(f"⚠️ Còn {remaining} acc")
            return
        if remaining <= 0:
            return await update.message.reply_text(f"🚫 Hết acc\n/key <mã>")
    processing_users.add(uid)
    msg = await update.message.reply_text(f"⏳ {n} acc...")
    accs = await fetch_fast(n)
    processing_users.discard(uid)
    if not accs:
        return await msg.edit_text("Không lấy được")
    u["history"] = u.get("history", []) + accs
    u["used"] = u.get("used", 0) + len(accs)
    u["daily_used"] = u.get("daily_used", 0) + len(accs)
    if uid != ADMIN_ID:
        await safe_save()
    await msg.edit_text(f"🎉 {len(accs)} acc\n\n{chr(10).join(accs)[:3800]}")

async def export(update, context):
    uid = update.effective_user.id
    if not is_auth(uid):
        return await update.message.reply_text("🔐 Chưa nhập key\nNhấn nút Lấy Key Free để được cấp key.")
    u = get_user(uid)
    history = u.get("history", [])
    if not history:
        return await update.message.reply_text("📭 Chưa lấy acc")
    ts = int(time.time())
    fn = f"{OUTPUT_DIR}/history_{uid}_{ts}.txt"
    with open(fn, "w", encoding="utf-8") as f:
        f.write("\n".join(history))
    with open(fn, "rb") as f:
        await context.bot.send_document(chat_id=uid, document=f, filename="lich_su_acc.txt",
                                        caption=f"📁 {len(history)} acc")
    await update.message.reply_text("✅ Đã gửi file!")

async def profile(update, context):
    uid = update.effective_user.id
    if not is_auth(uid):
        return await update.message.reply_text("🔐 Chưa nhập key\nNhấn nút Lấy Key Free để được cấp key.")
    u = get_user(uid)
    limit = get_limit(uid)
    vip = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
    await update.message.reply_text(f"👤 {vip}\n📊 {u.get('daily_used', 0)}/{limit}\n📦 Tổng: {u.get('used', 0)}")

async def getkey(update, context):
    await update.message.reply_text(
        "🔑 Nhấn nút *Lấy Key Free* để ib t.me/chantuiii nhận key miễn phí!\nSau khi có key: /key <mã>",
        parse_mode="Markdown")

async def genkey(update, context):
    if not is_admin(update.effective_user.id):
        return
    try:
        n = int(context.args[0]) if context.args else 1
    except:
        n = 1
    if n > 20:
        n = 20
    new_keys = []
    for _ in range(n):
        k = secrets.token_hex(8).upper()
        keys[k] = {"type": "normal", "created": datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),
                   "created_ts": time.time()}
        new_keys.append(k)
    await safe_save()
    await update.message.reply_text(f"🔑 Key thường ({LIMIT_NORMAL} acc/ngày):\n" + "\n".join(f"`{k}`" for k in new_keys),
                                    parse_mode="Markdown")

async def genvip(update, context):
    if not is_admin(update.effective_user.id):
        return
    try:
        n = int(context.args[0]) if context.args else 1
    except:
        n = 1
    if n > 10:
        n = 10
    new_keys = []
    for _ in range(n):
        k = secrets.token_hex(8).upper()
        keys[k] = {"type": "vip", "vip": True, "created": datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),
                   "created_ts": time.time()}
        new_keys.append(k)
    await safe_save()
    await update.message.reply_text(f"👑 Key VIP ({LIMIT_VIP} acc/ngày):\n" + "\n".join(f"`{k}`" for k in new_keys),
                                    parse_mode="Markdown")

async def key_status(update, context):
    if not is_admin(update.effective_user.id):
        return
    text = "📊 *Trạng thái Key*\n\n"
    if keys:
        text += "🔑 *Chưa dùng:*\n"
        for k, v in list(keys.items())[:15]:
            t = "VIP" if v.get("vip") else "TH"
            text += f"`{k}` ({t})\n"
    if users:
        text += "\n👥 *Đã dùng:*\n"
        for uid, u in list(users.items())[:20]:
            if uid == "admin" or uid == str(ADMIN_ID):
                continue
            name = u.get("name", uid)
            key = u.get("key", "?")[:8]
            vip = "VIP" if u.get("vip") else "TH"
            daily = u.get("daily_used", 0)
            limit = get_limit(int(uid))
            banned = "🚫" if u.get("banned") else ""
            text += f"👤 {name} | ID:`{uid}` | Key:{key}... ({vip}) | {daily}/{limit} {banned}\n"
    if not keys and not users:
        text += "📭 Chưa có dữ liệu"
    await update.message.reply_text(text, parse_mode="Markdown")

async def users_list(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not users:
        return await update.message.reply_text("📭 Chưa có user")
    text = "👥 *Danh sách User:*\n\n"
    for uid, u in list(users.items())[:30]:
        if uid == "admin" or uid == str(ADMIN_ID):
            continue
        name = u.get("name", uid)
        key = u.get("key", "?")[:8]
        vip = "VIP" if u.get("vip") else "TH"
        daily = u.get("daily_used", 0)
        limit = get_limit(int(uid))
        banned = "🚫" if u.get("banned") else "✅"
        text += f"👤 {name} | `{uid}` | {key}... ({vip}) | {daily}/{limit} {banned}\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def stats(update, context):
    if not is_admin(update.effective_user.id):
        return
    nk = sum(1 for k in keys.values() if not k.get("vip"))
    vk = sum(1 for k in keys.values() if k.get("vip"))
    total = sum(u.get("used", 0) for u in users.values())
    await update.message.reply_text(
        f"📊 Key: {nk}+{vk}VIP | 👥 {len(users)} | 📦 {total} | 📋 {len(used_accounts)} | 🤐 {len(muted_users)}")

async def listkeys(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not keys:
        return await update.message.reply_text("📭 Kho key trống")
    text = "🔑 *Kho key:*\n"
    for k, v in list(keys.items())[:20]:
        t = "VIP" if v.get("vip") else "TH"
        text += f"`{k}` ({t})\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def ban(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        return await update.message.reply_text("/ban <id>")
    uid = context.args[0]
    if uid in users:
        users[uid]["banned"] = True
        await safe_save()
        await update.message.reply_text(f"✅ Đã ban {uid}")
    else:
        await update.message.reply_text("❌ Không tìm thấy")

async def unban(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        return await update.message.reply_text("/unban <id>")
    uid = context.args[0]
    if uid in users:
        users[uid]["banned"] = False
        await safe_save()
        await update.message.reply_text(f"✅ Đã unban {uid}")
    else:
        await update.message.reply_text("❌ Không tìm thấy")

async def revoke(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        return await update.message.reply_text("/revoke <id>")
    uid = context.args[0]
    if uid in users:
        del users[uid]
        await safe_save()
        await update.message.reply_text(f"✅ Đã thu hồi key của {uid}")
    else:
        await update.message.reply_text("❌ Không tìm thấy")

async def reset(update, context):
    if not is_admin(update.effective_user.id):
        return
    if context.args:
        uid = context.args[0]
        if uid in users:
            users[uid]["daily_used"] = 0
            users[uid]["date"] = today_vn()
            await safe_save()
            await update.message.reply_text(f"✅ Đã reset {uid}")
        else:
            await update.message.reply_text("❌ Không tìm thấy")
    else:
        for u in users:
            users[u]["daily_used"] = 0
            users[u]["date"] = today_vn()
        await safe_save()
        await update.message.reply_text("✅ Đã reset tất cả")

async def delkey(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        return await update.message.reply_text("/delkey <key>")
    key = context.args[0].strip().upper()
    if key in keys:
        del keys[key]
        await safe_save()
        await update.message.reply_text("✅ Đã xóa key")
    else:
        await update.message.reply_text("❌ Key không tồn tại hoặc đã dùng")

async def muted_list(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not muted_users:
        return await update.message.reply_text("📭 Không có ai bị mute.")
    text = "🤐 *Danh sách Mute:*\n\n"
    now = time.time()
    for uid, until in list(muted_users.items())[:20]:
        remaining = int(until - now)
        if remaining > 0:
            name = get_user(int(uid)).get("name", str(uid))
            text += f"👤 {name} | `{uid}` | ⏳ {remaining}s\n"
    if text == "🤐 *Danh sách Mute:*\n\n":
        text += "📭 Không có ai bị mute."
    await update.message.reply_text(text, parse_mode="Markdown")

async def unmute_cmd(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        return await update.message.reply_text("/unmute <id>")
    uid = int(context.args[0])
    if uid in muted_users:
        del muted_users[uid]
        request_log[uid].clear()
        await update.message.reply_text(f"✅ Đã bỏ mute {uid}")
    else:
        await update.message.reply_text(f"❌ {uid} không bị mute")

async def resetall(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not context.args or context.args[0] != "CONFIRM":
        return await update.message.reply_text("⚠️ Xác nhận: /resetall CONFIRM")
    keys.clear()
    users.clear()
    used_accounts.clear()
    request_log.clear()
    muted_users.clear()
    processing_users.clear()
    await safe_save()
    log.warning("RESET ALL!")
    await update.message.reply_text("☢️ Đã xóa TOÀN BỘ dữ liệu!")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log.exception(context.error)

if __name__ == "__main__":
    log.info(f"🤖 LQ ACC BOT | Admin:{ADMIN_ID} | ⚡{CONCURRENT} req")
    asyncio.run(initialize_data())

    app_bot = Application.builder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(CommandHandler("getkey", getkey))
    app_bot.add_handler(CommandHandler("key", key_cmd))
    app_bot.add_handler(CommandHandler("lay", lay))
    app_bot.add_handler(CommandHandler("export", export))
    app_bot.add_handler(CommandHandler("profile", profile))
    app_bot.add_handler(CallbackQueryHandler(button_handler))
    app_bot.add_handler(CommandHandler("genkey", genkey))
    app_bot.add_handler(CommandHandler("genvip", genvip))
    app_bot.add_handler(CommandHandler("status", key_status))
    app_bot.add_handler(CommandHandler("users", users_list))
    app_bot.add_handler(CommandHandler("stats", stats))
    app_bot.add_handler(CommandHandler("keys", listkeys))
    app_bot.add_handler(CommandHandler("ban", ban))
    app_bot.add_handler(CommandHandler("unban", unban))
    app_bot.add_handler(CommandHandler("revoke", revoke))
    app_bot.add_handler(CommandHandler("reset", reset))
    app_bot.add_handler(CommandHandler("delkey", delkey))
    app_bot.add_handler(CommandHandler("muted", muted_list))
    app_bot.add_handler(CommandHandler("unmute", unmute_cmd))
    app_bot.add_handler(CommandHandler("resetall", resetall))
    app_bot.add_error_handler(error_handler)
    app_bot.run_polling(drop_pending_updates=True)
