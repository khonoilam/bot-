import asyncio, aiohttp, os, time, logging, secrets, pytz, json, threading, requests, copy
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ======================== FLASK KEEP-ALIVE ========================
app = Flask(__name__)
@app.route("/")
def home(): return "Bot đang chạy!"
def web(): app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# ======================== CONFIG ========================
BOT_TOKEN = "8258187122:AAExWr8i1jAeqZJbxnWkLW39gGA_FQN3I1I"
ADMIN_ID = 8721023843
TANGACC_TOKEN = "https://tangacc.net/token.php"
TANGACC_ACC   = "https://tangacc.net/get_lq_acc.php"
FETCH_TIMEOUT = 30; FETCH_WORKERS = 20; FETCH_SEMAPHORE = asyncio.Semaphore(50)

# API Check (3 cái, chạy song song race)
CHECK_API_1 = "http://103.77.246.176:5000/check"
CHECK_API_2 = "http://160.22.107.245:5000/check"
CHECK_API_3 = "http://160.22.107.245:5000/check"
CHECK_API_KEY = "RSAEJ8rdtRaMLfVUCB70Mh8pL0SFSDDx"

JSONBIN_API_KEY = "$2a$10$ZKItx9kCcaQktuLuBDKY1ewYhT2gy3OWH.w7nkeTLWUy9sCxtjVWO"
JSONBIN_BIN_ID = "6a6b41e8da38895dfea40e00"
JSONBIN_API_URL = "https://api.jsonbin.io/v3/b"
LIMIT_NORMAL = 100; LIMIT_VIP = 550; CHECK_LIMIT_NORMAL = 25; CHECK_LIMIT_VIP = 75
MAX_PER_REQ = 50; KEY_EXPIRE_SECONDS = 86400
TZ = pytz.timezone("Asia/Ho_Chi_Minh")
SPAM_WINDOW = 10; MAX_SPAM = 5; MUTE_TIME = 60
CHECK_TIMEOUT = 60                # API rất chậm, cần 60s
CHECK_CONCURRENT = 5              # Giới hạn 5 request đồng thời
OUTPUT_DIR = "lq_data"; MAX_HISTORY = 2000; MAX_LAST_ACC = 500; MAX_CHECKED_ACC = 500
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
log = logging.getLogger(__name__)

global_executor = ThreadPoolExecutor(max_workers=50)
keys_lock = threading.Lock()
users_lock = threading.Lock()
acc_lock = asyncio.Lock()
spam_lock = threading.Lock()
data_lock = threading.Lock()

keys = {}; users = {}; used_accounts = set(); active_tasks = {}

HEADERS = {"authority":"tangacc.net","accept":"*/*","accept-language":"en-US,en;q=0.9","referer":"https://tangacc.net/","user-agent":"Mozilla/5.0"}

def today_vn(): return datetime.now(TZ).strftime("%Y-%m-%d")
def is_admin(uid): return uid == ADMIN_ID

def _clean_expired_users_nolock():
    now = time.time()
    for uid in [u for u, data in users.items() if data.get("expire_ts") and now > data["expire_ts"]]: del users[uid]

def _get_user_copy(uid):
    if uid == ADMIN_ID:
        if "admin" not in users: users["admin"] = {"history":[],"used":0,"daily_used":0,"banned":False,"vip":True,"last_acc":[],"checked_acc":[],"expire_ts":0}
        return copy.deepcopy(users["admin"])
    u = users.get(str(uid))
    if u:
        if u.get("expire_ts") and time.time() > u["expire_ts"]: del users[str(uid)]; return {}
        return copy.deepcopy(u)
    return {}

def load_data():
    global keys, users, used_accounts
    with data_lock:
        try:
            r = requests.get(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}/latest", headers={"X-Master-Key":JSONBIN_API_KEY}, timeout=10)
            if r.status_code == 200:
                data = r.json().get("record",{})
                with keys_lock: keys = data.get("keys",{})
                with users_lock: users = data.get("users",{}); _clean_expired_users_nolock()
                used_accounts = set(data.get("used_accounts",[]))
        except: pass

def save_data_sync():
    with data_lock:
        for _ in range(2):
            try:
                with keys_lock: k = copy.deepcopy(keys)
                with users_lock: u = copy.deepcopy(users)
                a = list(used_accounts)
                requests.put(f"{JSONBIN_API_URL}/{JSONBIN_BIN_ID}", json={"keys":k,"users":u,"used_accounts":a},
                             headers={"X-Master-Key":JSONBIN_API_KEY,"Content-Type":"application/json"}, timeout=10)
                return
            except: time.sleep(1)

async def save_data():
    await asyncio.get_event_loop().run_in_executor(global_executor, save_data_sync)

def get_acc_sync():
    try:
        with requests.Session() as s:
            r = s.get(TANGACC_TOKEN, headers=HEADERS, timeout=5)
            token = r.text.strip()
            if not token: return None
            h = {**HEADERS, "content-type":"application/x-www-form-urlencoded", "origin":"https://tangacc.net"}
            r2 = s.post(TANGACC_ACC, headers=h, data={"token":token}, timeout=5)
            acc = r2.text.strip()
            if acc and "|" in acc and not acc.startswith("WAIT") and not acc.startswith("ERROR"): return acc
    except: pass
    return None

async def fetch_fast_async(n):
    acc_set = set(); live_accs = []; lock = asyncio.Lock()
    async def worker():
        while True:
            async with lock:
                if len(live_accs) >= n: return
            async with FETCH_SEMAPHORE:
                acc = await asyncio.get_event_loop().run_in_executor(global_executor, get_acc_sync)
            if not acc: continue
            async with lock:
                if len(live_accs) >= n: return
                if acc in acc_set: continue
                async with acc_lock:
                    if acc in used_accounts: continue
                    used_accounts.add(acc)
                acc_set.add(acc); live_accs.append(acc)
    tasks = [asyncio.create_task(worker()) for _ in range(FETCH_WORKERS)]
    try: await asyncio.wait_for(asyncio.gather(*tasks), timeout=FETCH_TIMEOUT)
    except asyncio.TimeoutError:
        for t in tasks: t.cancel()
    return live_accs[:n]

# ======================== CHECK ACC (SONG SONG 3 API) ========================
async def _call_api(url, username, password, apikey=None):
    """Gọi 1 API, trả về dict kết quả hoặc None nếu lỗi"""
    data = {"user": username, "pass": password}
    if apikey: data["apikey"] = apikey
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=CHECK_TIMEOUT)) as resp:
                if resp.status != 200: return None
                d = await resp.json()
                if not d.get("ok") or "result" not in d: return None
                if d["result"].get("status") == "HIT":
                    info = d["result"]; skins = info.get("aov_skins", {})
                    ss=skins.get("ss",0); sss=skins.get("sss",0); anime=skins.get("anime",0)
                    rare=[]
                    if ss: rare.append(f"SS:{','.join(skins.get('ss_list',[]))}")
                    if sss: rare.append(f"SSS:{','.join(skins.get('sss_list',[]))}")
                    if anime: rare.append(f"Anime:{','.join(skins.get('anime_list',[]))}")
                    rare_str=" | ".join(rare) if rare else "Không"
                    ban=info.get("aov_banned","NO"); ban_start=""; ban_end=""
                    if ban=="YES":
                        kt=info.get("_kt_player",{}).get("banInfo",{})
                        bt=kt.get("banTime",0); ut=kt.get("unbanTime",0)
                        if bt:
                            ban_start=datetime.utcfromtimestamp(bt).strftime("%d/%m/%Y")
                            ban_end=datetime.utcfromtimestamp(ut).strftime("%d/%m/%Y") if ut else "Vĩnh viễn"
                    return {"status":"HIT","username":username,"name":info.get("aov_name","?"),
                            "uid":info.get("uid","?"),"rank":info.get("aov_rank","?"),"level":info.get("aov_level","?"),
                            "skins":skins.get("total_skins",0),"champs":skins.get("total_champs",0),
                            "shells":info.get("shells",0),"banned":ban,"ban_start":ban_start,"ban_end":ban_end,
                            "fb_linked":"Yes" if info.get("fb_linked") else "No",
                            "mobile_bound":"Yes" if info.get("mobile_bound") else "No",
                            "email_verified":"Yes" if info.get("email_verified") else "No","rare":rare_str}
                else:
                    return {"status":"MISS","username":username,"message":d["result"].get("detail","Sai mật khẩu / không tồn tại")}
    except: return None

async def check_acc_race(username, password):
    """Chạy 3 API song song, lấy kết quả nhanh nhất"""
    tasks = [
        _call_api(CHECK_API_1, username, password),
        _call_api(CHECK_API_2, username, password),
        _call_api(CHECK_API_3, username, password, apikey=CHECK_API_KEY),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    # Hủy các task còn lại
    for task in pending: task.cancel()
    for task in done:
        result = task.result()
        if result is not None:
            return result
    return {"status":"ERROR","username":username,"message":"Tất cả API check đều lỗi"}

# ======================== SPAM ========================
request_log = defaultdict(list); muted_users = {}
def check_spam(uid):
    if uid == ADMIN_ID: return False
    now = time.time()
    with spam_lock:
        if uid in muted_users:
            if now < muted_users[uid]: return True
            else: del muted_users[uid]
        times = request_log[uid]
        times = [t for t in times if now - t < SPAM_WINDOW]; times.append(now)
        request_log[uid] = times
        if len(times) > MAX_SPAM:
            muted_users[uid] = now + MUTE_TIME; return True
    return False

def main_menu(uid):
    with users_lock:
        u = _get_user_copy(uid)
        if not u: return None, None
        if uid == ADMIN_ID: limit=99999; check_rem=99999
        else:
            limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL
            today = today_vn()
            if u.get("last_check_date") != today: check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
            else: check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today",0))
        used = u.get("daily_used",0); remaining = max(0, limit - used)
        vip_text = "👑 VIP" if u.get("vip") else "🔑 Thường"
    keyboard = [
        [InlineKeyboardButton("🎮 Lấy 10 Acc", callback_data="lay_10"), InlineKeyboardButton("🎮 Lấy 30 Acc", callback_data="lay_30")],
        [InlineKeyboardButton("🎮 Lấy 50 Acc", callback_data="lay_50")],
        [InlineKeyboardButton("📁 Xuất Acc Thô", callback_data="export"), InlineKeyboardButton("📁 Xuất Acc Check", callback_data="export_checked")],
        [InlineKeyboardButton("👤 Profile", callback_data="profile")],
    ]
    text = f"🤖 LQ ACC BOT\n{vip_text}\n📊 Hôm nay: {used}/{limit} (Còn {remaining})\n🔍 Check: {check_rem} lượt\n⚡TangAcc\nChọn chức năng:"
    return InlineKeyboardMarkup(keyboard), text

def admin_menu_text():
    return ("👑 ADMIN MENU\n\n"
            "/genkey - Tạo key thường (chưa dùng không hết hạn)\n"
            "/genvip - Tạo key VIP (chưa dùng không hết hạn)\n"
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
            "/delkey <key> - Xóa key chưa dùng\n"
            "/resetall CONFIRM - Xóa toàn bộ dữ liệu")

def login_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 Nhập Key", callback_data="key_input")],
        [InlineKeyboardButton("🔗 Lấy Key Free", url="https://t.me/chantuiii")],
    ])

def update_check_count(uid, count, hit_details=None):
    uid = str(uid)
    if uid not in users: return
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    with users_lock:
        user = users[uid]
        if user.get("last_check_date") != today: user["last_check_date"] = today; user["checked_today"] = count
        else: user["checked_today"] = user.get("checked_today",0) + count
        if hit_details:
            checked = user.setdefault("checked_acc", [])
            checked.extend(hit_details)
            if len(checked) > MAX_CHECKED_ACC: user["checked_acc"] = checked[-MAX_CHECKED_ACC:]

async def start(update, context):
    uid = update.effective_user.id
    if uid != ADMIN_ID and check_spam(uid): await update.message.reply_text("🚫 Spam! 60s."); return
    user = update.effective_user; name = user.full_name or user.username or str(uid)
    need_save = False
    with users_lock:
        if str(uid) in users:
            u = users[str(uid)]
            if u.get("expire_ts") and time.time() > u["expire_ts"]: del users[str(uid)]; need_save = True; auth = False
            else:
                if u.get("name") != name: u["name"] = name; need_save = True
                auth = True
        else: auth = False
    if need_save: await save_data()
    if uid == ADMIN_ID: await update.message.reply_text(admin_menu_text())
    elif auth:
        kb, text = main_menu(uid); await update.message.reply_text(text, reply_markup=kb)
    else: await update.message.reply_text("🤖 LQ ACC BOT\n\n🔐 Chưa có key.\n👉 Nhấn nút \"Lấy Key Free\" bên dưới.", reply_markup=login_menu())

async def button_handler(update, context):
    query = update.callback_query; await query.answer()
    uid = query.from_user.id; data = query.data

    with users_lock:
        if uid == ADMIN_ID: auth = True
        else:
            u = users.get(str(uid))
            if u and u.get("expire_ts") and time.time() > u["expire_ts"]: del users[str(uid)]; await save_data(); auth = False
            elif u and u.get("key"): auth = True
            else: auth = False
        if not auth and data != "key_input":
            await query.edit_message_text("🔐 Chưa nhập key hoặc key đã hết hạn!", reply_markup=login_menu()); return
        if auth and uid != ADMIN_ID:
            if users.get(str(uid), {}).get("banned"): await query.edit_message_text("🚫 Bạn đã bị khóa!"); return
    if check_spam(uid): await query.edit_message_text("🚫 Spam!"); return

    if data.startswith("lay_"):
        n = int(data.split("_")[1])
        if uid != ADMIN_ID and n > MAX_PER_REQ: await query.answer(f"Tối đa {MAX_PER_REQ}", show_alert=True); n = MAX_PER_REQ
        with users_lock:
            if uid == ADMIN_ID: limit = 99999
            else:
                u = users[str(uid)]
                if u.get("date") != today_vn(): u["date"] = today_vn(); u["daily_used"] = 0
                limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL; remaining = limit - u.get("daily_used",0)
                if remaining <= 0: await query.edit_message_text(f"🚫 Hết {limit} acc/ngày"); return
                if n > remaining: n = remaining
        if uid in active_tasks and not active_tasks[uid].done():
            await query.answer("Đang lấy acc, vui lòng đợi.", show_alert=True); return
        wait_msg = await query.edit_message_text(f"⏳ Đang lấy {n} acc...")

        async def fetch_and_send():
            try:
                accs = await fetch_fast_async(n)
                if not accs: await context.bot.edit_message_text("❌ Không lấy được acc.", chat_id=uid, message_id=wait_msg.message_id); return
                with users_lock:
                    if uid == ADMIN_ID: u = users.setdefault("admin", {"history":[],"used":0,"daily_used":0,"banned":False,"vip":True,"last_acc":[],"checked_acc":[],"expire_ts":0})
                    else: u = users[str(uid)]
                    u["history"] = (u.get("history",[]) + accs)[-MAX_HISTORY:]; u["used"] = u.get("used",0) + len(accs)
                    u["daily_used"] = u.get("daily_used",0) + len(accs); u["last_acc"] = accs.copy()
                    last_copy = list(accs); total_acc = len(last_copy)
                if uid != ADMIN_ID: await save_data()
                with users_lock:
                    if uid == ADMIN_ID: check_rem = 99999
                    else:
                        u = users[str(uid)]; today = today_vn()
                        if u.get("last_check_date") != today: check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
                        else: check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today",0))
                can_check = min(total_acc, check_rem); check_buttons = []
                if can_check >= 5: check_buttons.append(InlineKeyboardButton("✅ Check 5", callback_data="checkacc_5"))
                if can_check >= 3: check_buttons.append(InlineKeyboardButton("Check 3", callback_data="checkacc_3"))
                reply_markup = InlineKeyboardMarkup([check_buttons]) if check_buttons else None

                acc_text = "\n".join(last_copy)
                if len(acc_text) > 3800 or total_acc > 20:
                    ts = int(time.time()); fn = f"{OUTPUT_DIR}/acclist_{uid}_{ts}.txt"
                    with open(fn,"w",encoding="utf-8") as f: f.write("\n".join(last_copy))
                    with open(fn,"rb") as f: await context.bot.send_document(chat_id=uid, document=f, filename=f"acc_list_{ts}.txt", caption=f"📁 {total_acc} acc")
                    msg_text = f"✅ Đã lấy {total_acc} acc\n📎 File bên dưới."
                    if check_buttons: msg_text += "\nChọn số acc muốn check:"
                else:
                    msg_text = f"🎉 {total_acc} acc\n\n{acc_text}"
                    if check_buttons: msg_text += "\nChọn số acc muốn check:"
                await context.bot.edit_message_text(msg_text, chat_id=uid, message_id=wait_msg.message_id, reply_markup=reply_markup)
                kb, menu_text = main_menu(uid)
                if kb: await context.bot.send_message(chat_id=uid, text=menu_text, reply_markup=kb)
            except Exception as e: log.error(f"Lỗi fetch background: {e}")
        active_tasks[uid] = asyncio.create_task(fetch_and_send())

    elif data.startswith("checkacc_"):
        parts = data.split("_")
        if len(parts) != 2: return
        req_count = int(parts[1])
        if req_count > 5: req_count = 5
        original_msg = query.message
        with users_lock:
            u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {"last_acc":[]})
            acc_list = list(u.get("last_acc", []))
            if not acc_list: await query.edit_message_text("⚠️ Vui lòng lấy acc trước."); return
            today = today_vn()
            if u.get("last_check_date") != today: remaining = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
            else: remaining = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today",0))
            can_check = min(req_count, len(acc_list), remaining)
            if can_check <= 0: await query.edit_message_text("🚫 Hết lượt check."); return
            accs_to_check = acc_list[:can_check]
        if uid in active_tasks and not active_tasks[uid].done():
            await query.answer("Đang xử lý, vui lòng đợi.", show_alert=True); return
        wait_msg = await context.bot.send_message(chat_id=uid, text=f"🔍 Đang check {can_check} acc...")

        async def check_and_send():
            try:
                sem = asyncio.Semaphore(CHECK_CONCURRENT)
                async def limited_check(acc_str):
                    if "|" not in acc_str: return {"status":"ERROR"}
                    uname, pwd = acc_str.split("|",1)
                    async with sem: return await check_acc_race(uname, pwd)
                results = await asyncio.gather(*[limited_check(a) for a in accs_to_check])
                success_results = [r for r in results if r["status"] in ("HIT","MISS")]
                hit_details = []
                for r in results:
                    if r["status"] == "HIT":
                        hit_details.append({
                            "username":r["username"],"name":r.get("name","?"),"rank":r.get("rank","?"),
                            "level":r.get("level","?"),"skins":r.get("skins",0),"champs":r.get("champs",0),
                            "banned":r.get("banned","NO"),"ban_start":r.get("ban_start",""),"ban_end":r.get("ban_end",""),
                            "fb_linked":r.get("fb_linked","?"),"mobile_bound":r.get("mobile_bound","?"),
                            "email_verified":r.get("email_verified","?"),"rare":r.get("rare","Không"),"shells":r.get("shells",0)
                        })
                with users_lock:
                    u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin", {})
                    if success_results: update_check_count(uid, len(success_results), hit_details)
                    new_last = acc_list[can_check:] if len(acc_list) > can_check else []
                    u["last_acc"] = new_last
                if uid != ADMIN_ID: await save_data()
                with users_lock:
                    u2 = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin",{})
                    new_total = len(new_last); today = today_vn()
                    if u2.get("last_check_date") != today: check_rem = CHECK_LIMIT_VIP if u2.get("vip") else CHECK_LIMIT_NORMAL
                    else: check_rem = max(0, (CHECK_LIMIT_VIP if u2.get("vip") else CHECK_LIMIT_NORMAL) - u2.get("checked_today",0))
                can_check_new = min(new_total, check_rem); new_check_buttons = []
                if can_check_new >= 5: new_check_buttons.append(InlineKeyboardButton("✅ Check 5", callback_data="checkacc_5"))
                if can_check_new >= 3: new_check_buttons.append(InlineKeyboardButton("Check 3", callback_data="checkacc_3"))
                new_reply_markup = InlineKeyboardMarkup([new_check_buttons]) if new_check_buttons else None
                new_acc_text = "\n".join(new_last[:20])
                if new_total > 20: new_acc_text += f"\n... và {new_total-20} acc khác"
                new_msg_text = f"🎉 Còn {new_total} acc chưa check\n\n{new_acc_text}" if new_total > 0 else "✅ Đã check hết acc."
                try: await original_msg.edit_text(new_msg_text, reply_markup=new_reply_markup)
                except Exception as e: log.error(f"Không thể cập nhật: {e}")
                hit = [r for r in results if r["status"] == "HIT"]; miss = [r for r in results if r["status"] != "HIT"]
                text = f"📊 Kết quả check {len(results)} acc\n\n✅ HIT: {len(hit)}\n❌ MISS/ERROR: {len(miss)}\n"
                if hit:
                    text += "\nChi tiết HIT:\n"
                    for r in hit:
                        ban_text = "Không"
                        if r.get("banned") == "YES":
                            ban_text = "Có"
                            if r.get("ban_start") and r.get("ban_end"): ban_text += f" ({r['ban_start']} → {r['ban_end']})"
                            elif r.get("ban_start"): ban_text += f" (từ {r['ban_start']})"
                        text += (f"👤 {r['username']} - {r.get('name','?')}\n   Rank: {r.get('rank','?')} | Lv: {r.get('level','?')}\n   Skin: {r.get('skins',0)} | Tướng: {r.get('champs',0)} | Sò: {r.get('shells',0)}\n   Ban: {ban_text}\n   FB: {r.get('fb_linked','?')} | SĐT: {r.get('mobile_bound','?')} | Email: {r.get('email_verified','?')}\n   Hiếm: {r.get('rare','Không')}\n")
                if miss:
                    text += "\nMISS/ERROR:\n"
                    for r in miss[:5]: text += f"❌ {r['username']} - {r.get('message','?')}\n"
                await wait_msg.edit_text(text)
                kb, menu_text = main_menu(uid)
                if kb: await context.bot.send_message(chat_id=uid, text=menu_text, reply_markup=kb)
            except Exception as e: log.error(f"Lỗi check background: {e}")
        active_tasks[uid] = asyncio.create_task(check_and_send())

    elif data == "export":
        with users_lock: u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin",{"history":[]}); history = list(u.get("history",[]))
        if not history: await query.edit_message_text("📭 Chưa lấy acc nào."); return
        ts=int(time.time()); fn=f"{OUTPUT_DIR}/acc_tho_{uid}_{ts}.txt"
        with open(fn,"w",encoding="utf-8") as f: f.write("\n".join(history))
        with open(fn,"rb") as f: await context.bot.send_document(chat_id=uid, document=f, filename=f"acc_tho_{ts}.txt", caption=f"📁 {len(history)} acc thô")
        await query.edit_message_text("✅ Đã gửi file!")
    elif data == "export_checked":
        with users_lock: u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin",{"checked_acc":[]}); checked = list(u.get("checked_acc",[]))
        if not checked: await query.edit_message_text("📭 Chưa check acc nào."); return
        ts=int(time.time()); fn=f"{OUTPUT_DIR}/acc_check_{uid}_{ts}.txt"
        with open(fn,"w",encoding="utf-8") as f:
            for item in checked:
                f.write(f"User:{item['username']}\nName:{item['name']}\nRank:{item['rank']} Lv:{item['level']}\nSkin:{item['skins']} Tướng:{item['champs']} Sò:{item.get('shells',0)}\nBan:{item['banned']}")
                if item['banned']=='YES' and item.get('ban_start'): f.write(f" ({item['ban_start']}->{item['ban_end']})")
                f.write(f"\nFB:{item['fb_linked']} SĐT:{item['mobile_bound']} Email:{item['email_verified']}\nHiếm:{item.get('rare','Không')}\n{'─'*30}\n")
        with open(fn,"rb") as f: await context.bot.send_document(chat_id=uid, document=f, filename=f"acc_check_{ts}.txt", caption=f"📁 {len(checked)} acc check")
        await query.edit_message_text("✅ Đã gửi file!")
    elif data == "profile":
        with users_lock:
            u = _get_user_copy(uid)
            if not u: await query.edit_message_text("🔐 Vui lòng nhập key."); return
            if uid == ADMIN_ID: limit=99999; check_rem=99999; expire_str="Vĩnh viễn"
            else:
                limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL; today = today_vn()
                if u.get("last_check_date") != today: check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
                else: check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today",0))
                if u.get("expire_ts"): rs = u["expire_ts"] - time.time(); expire_str = f"{int(rs//3600)}h{int((rs%3600)//60)}m" if rs > 0 else "Hết hạn"
                else: expire_str = "Không xác định"
            vip_text = "👑 Admin" if uid == ADMIN_ID else ("👑 VIP" if u.get("vip") else "🔑 Thường")
            total_acc = len(u.get("last_acc",[])); checked_count = len(u.get("checked_acc",[]))
        await query.edit_message_text(f"👤 {vip_text}\n⏳ Hết hạn: {expire_str}\n📊 Lấy: {u.get('daily_used',0)}/{limit}\n📦 Tổng acc: {u.get('used',0)}\n🔍 Check còn: {check_rem}\n📋 Chờ check: {total_acc}\n✅ Đã check: {checked_count}")
    elif data == "key_input":
        with users_lock:
            if uid == ADMIN_ID: auth = True
            else: u = users.get(str(uid)); auth = u and u.get("key") and not (u.get("expire_ts") and time.time() > u["expire_ts"])
        if auth: await query.answer("Bạn đã có key rồi!", show_alert=True)
        else: await query.edit_message_text("📝 Gửi key: /key <mã>")

async def key_cmd(update, context):
    uid = str(update.effective_user.id)
    if not context.args: await update.message.reply_text("🔐 Key không đúng!"); return
    key = context.args[0].strip().upper()
    with keys_lock:
        if key not in keys: await update.message.reply_text("🔐 Key không đúng hoặc đã được dùng."); return
        kd = keys[key]
        if kd.get("banned"): await update.message.reply_text("❌ Key bị khóa"); return
        is_vip = kd.get("vip", False); del keys[key]
    with users_lock:
        if uid in users: del users[uid]
        users[uid] = {"key":key,"vip":is_vip,"used":0,"daily_used":0,"date":today_vn(),"banned":False,"history":[],"last_check_date":today_vn(),"checked_today":0,"name":update.effective_user.full_name or uid,"last_acc":[],"checked_acc":[],"expire_ts":time.time()+KEY_EXPIRE_SECONDS}
    await save_data()
    limit = LIMIT_VIP if is_vip else LIMIT_NORMAL
    kb, text = main_menu(int(uid))
    await update.message.reply_text(f"✅ {'👑 VIP' if is_vip else '🔑 Thường'} | {limit} acc/ngày\nCheck: {CHECK_LIMIT_VIP if is_vip else CHECK_LIMIT_NORMAL}/ngày\n⏳ Key tồn tại 24h.", reply_markup=kb)

async def lay(update, context):
    uid = update.effective_user.id
    with users_lock:
        if uid == ADMIN_ID: auth=True
        else: u=users.get(str(uid)); auth=u and u.get("key") and not (u.get("expire_ts") and time.time()>u["expire_ts"])
    if not auth: await update.message.reply_text("🔐 Chưa nhập key hoặc key hết hạn."); return
    if check_spam(uid): await update.message.reply_text("🚫 Spam!"); return
    with users_lock:
        u = users.get(str(uid)) if uid != ADMIN_ID else users.setdefault("admin",{})
        if u.get("banned"): await update.message.reply_text("🚫 Bị khóa"); return
        if uid != ADMIN_ID:
            if u.get("date") != today_vn(): u["date"]=today_vn(); u["daily_used"]=0
            limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL; remaining = limit - u.get("daily_used",0)
            try: n=int(context.args[0]) if context.args else 10
            except: n=10
            if n > remaining: await update.message.reply_text(f"⚠️ Còn {remaining} acc"); return
        else:
            try: n=int(context.args[0]) if context.args else 10
            except: n=10
        if n > MAX_PER_REQ and uid != ADMIN_ID: await update.message.reply_text(f"Tối đa {MAX_PER_REQ}"); n=MAX_PER_REQ
    if uid in active_tasks and not active_tasks[uid].done(): await update.message.reply_text("⏳ Đang lấy acc, vui lòng đợi."); return
    wait_msg = await update.message.reply_text(f"⏳ {n} acc...")
    async def fetch_and_send():
        try:
            accs = await fetch_fast_async(n)
            if not accs: await context.bot.edit_message_text("❌ Không lấy được acc.", chat_id=uid, message_id=wait_msg.message_id); return
            with users_lock:
                if uid == ADMIN_ID: u = users.setdefault("admin",{"history":[],"used":0,"daily_used":0,"banned":False,"vip":True,"last_acc":[],"checked_acc":[],"expire_ts":0})
                else: u = users[str(uid)]
                u["history"] = (u.get("history",[]) + accs)[-MAX_HISTORY:]; u["used"] = u.get("used",0)+len(accs)
                u["daily_used"] = u.get("daily_used",0)+len(accs); u["last_acc"] = accs.copy()
                last_copy = list(accs); total_acc = len(last_copy)
            if uid != ADMIN_ID: await save_data()
            with users_lock:
                if uid == ADMIN_ID: check_rem = 99999
                else:
                    u = users[str(uid)]; today = today_vn()
                    if u.get("last_check_date") != today: check_rem = CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL
                    else: check_rem = max(0, (CHECK_LIMIT_VIP if u.get("vip") else CHECK_LIMIT_NORMAL) - u.get("checked_today",0))
            can_check = min(total_acc, check_rem); check_buttons = []
            if can_check >= 5: check_buttons.append(InlineKeyboardButton("✅ Check 5", callback_data="checkacc_5"))
            if can_check >= 3: check_buttons.append(InlineKeyboardButton("Check 3", callback_data="checkacc_3"))
            reply_markup = InlineKeyboardMarkup([check_buttons]) if check_buttons else None
            acc_text = "\n".join(last_copy)
            if len(acc_text) > 3800 or total_acc > 20:
                ts=int(time.time()); fn=f"{OUTPUT_DIR}/acclist_{uid}_{ts}.txt"
                with open(fn,"w",encoding="utf-8") as f: f.write("\n".join(last_copy))
                with open(fn,"rb") as f: await context.bot.send_document(chat_id=uid, document=f, filename=f"acc_list_{ts}.txt", caption=f"📁 {total_acc} acc")
                msg_text = f"✅ Đã lấy {total_acc} acc\n📎 File bên dưới."
                if check_buttons: msg_text += "\nChọn số acc muốn check:"
            else:
                msg_text = f"🎉 {total_acc} acc\n\n{acc_text}"
                if check_buttons: msg_text += "\nChọn số acc muốn check:"
            await context.bot.edit_message_text(msg_text, chat_id=uid, message_id=wait_msg.message_id, reply_markup=reply_markup)
            kb, menu_text = main_menu(uid)
            if kb: await context.bot.send_message(chat_id=uid, text=menu_text, reply_markup=kb)
        except Exception as e: log.error(f"Lỗi fetch background: {e}")
    active_tasks[uid] = asyncio.create_task(fetch_and_send())

# ======================== ADMIN HANDLERS ========================
async def genkey(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 20: n = 20
    with keys_lock:
        new = [secrets.token_hex(8).upper() for _ in range(n)]
        for k in new: keys[k] = {"type":"normal","created_ts":time.time(),"banned":False,"vip":False}
    await save_data()
    await update.message.reply_text(f"🔑 Key thường ({LIMIT_NORMAL}/ngày) - Chưa dùng không hết hạn:\n" + "\n".join(new))
async def genvip(update, context):
    if not is_admin(update.effective_user.id): return
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    if n > 10: n = 10
    with keys_lock:
        new = [secrets.token_hex(8).upper() for _ in range(n)]
        for k in new: keys[k] = {"type":"vip","vip":True,"created_ts":time.time(),"banned":False}
    await save_data()
    await update.message.reply_text(f"👑 Key VIP ({LIMIT_VIP}/ngày) - Chưa dùng không hết hạn:\n" + "\n".join(new))
async def key_status(update, context):
    if not is_admin(update.effective_user.id): return
    with keys_lock: k_text = "\n".join(f"{k} ({'VIP' if v.get('vip') else 'TH'})" for k,v in list(keys.items())[:15]) if keys else ""
    with users_lock:
        u_text = ""
        for uid, u in list(users.items())[:20]:
            if uid in ("admin",str(ADMIN_ID)): continue
            limit = LIMIT_VIP if u.get("vip") else LIMIT_NORMAL; expire_str = ""
            if u.get("expire_ts"):
                rs = u["expire_ts"] - time.time()
                expire_str = f" - Còn {int(rs//3600)}h{int((rs%3600)//60)}m" if rs > 0 else " - Hết hạn"
            u_text += f"👤 {u.get('name',uid)} | {uid} | {u.get('key','?')[:8]}... ({'VIP' if u.get('vip') else 'TH'}) | {u.get('daily_used',0)}/{limit}{expire_str}\n"
    t = "📊 Trạng thái\n\n"
    if k_text: t += "🔑 Chưa dùng:\n" + k_text
    if u_text: t += "\n👥 Đang dùng:\n" + u_text
    if not k_text and not u_text: t += "📭 Trống"
    await update.message.reply_text(t)
async def users_list(update, context):
    if not is_admin(update.effective_user.id): return
    with users_lock:
        if not users: await update.message.reply_text("📭 Trống"); return
        u_list = []
        for uid, u in users.items():
            if uid in ("admin",str(ADMIN_ID)): continue
            expire_str = ""
            if u.get("expire_ts"):
                rs = u["expire_ts"] - time.time()
                expire_str = f" (còn {int(rs//3600)}h{int((rs%3600)//60)}m)" if rs > 0 else " (hết hạn)"
            u_list.append(f"👤 {u.get('name',uid)} | {uid} | {u.get('key','?')[:8]}...{expire_str}")
    await update.message.reply_text("👥 Users:\n" + "\n".join(u_list[:30]))
async def stats(update, context):
    if not is_admin(update.effective_user.id): return
    with keys_lock: nk = sum(1 for v in keys.values() if not v.get("vip")); vk = sum(1 for v in keys.values() if v.get("vip"))
    with users_lock: total_used = sum(u.get("used",0) for u in users.values()); user_count = len(users)
    with acc_lock: acc_count = len(used_accounts)
    with spam_lock: muted_count = len(muted_users)
    await update.message.reply_text(f"📊 Key: {nk}+{vk}VIP | 👥 {user_count} | 📦 {total_used} | 📋 {acc_count} | 🤐 {muted_count}")
async def listkeys(update, context):
    if not is_admin(update.effective_user.id): return
    with keys_lock:
        if not keys: await update.message.reply_text("📭 Trống"); return
        k_list = [f"{k} ({'VIP' if v.get('vip') else 'TH'})" for k,v in list(keys.items())[:20]]
    await update.message.reply_text("🔑 Kho key (chưa dùng):\n" + "\n".join(k_list))
async def ban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    with users_lock:
        if uid in users: users[uid]["banned"] = True; await save_data(); await update.message.reply_text(f"✅ Ban {uid}")
        else: await update.message.reply_text("❌ Không tìm thấy")
async def unban(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    with users_lock:
        if uid in users: users[uid]["banned"] = False; await save_data(); await update.message.reply_text(f"✅ Unban {uid}")
        else: await update.message.reply_text("❌ Không tìm thấy")
async def revoke(update, context):
    if not is_admin(update.effective_user.id): return
    uid = context.args[0] if context.args else ""
    with users_lock:
        if uid in users: del users[uid]; await save_data(); await update.message.reply_text(f"✅ Thu hồi {uid}")
        else: await update.message.reply_text("❌ Không tìm thấy")
async def reset(update, context):
    if not is_admin(update.effective_user.id): return
    if context.args:
        uid = context.args[0]
        with users_lock:
            if uid in users: users[uid]["daily_used"] = 0; await save_data(); await update.message.reply_text(f"✅ Reset {uid}")
            else: await update.message.reply_text("❌ Không tìm thấy")
    else:
        with users_lock:
            for u in users.values(): u["daily_used"] = 0
        await save_data(); await update.message.reply_text("✅ Reset hết")
async def delkey(update, context):
    if not is_admin(update.effective_user.id): return
    key = context.args[0].strip().upper() if context.args else ""
    with keys_lock:
        if key in keys: del keys[key]; await save_data(); await update.message.reply_text("✅ Đã xóa key")
        else: await update.message.reply_text("❌ Không tìm thấy key chưa dùng")
async def muted_list(update, context):
    if not is_admin(update.effective_user.id): return
    with spam_lock:
        if not muted_users: await update.message.reply_text("📭 Không ai mute"); return
        now = time.time(); lines = []
        for uid, until in muted_users.items():
            if until > now:
                with users_lock: name = users.get(str(uid), {}).get("name", str(uid))
                lines.append(f"👤 {name} | {uid} | ⏳{int(until-now)}s")
        await update.message.reply_text(f"🤐 Mute:\n\n" + "\n".join(lines[:20]))
async def unmute_cmd(update, context):
    if not is_admin(update.effective_user.id): return
    uid = int(context.args[0]) if context.args else 0
    with spam_lock:
        if uid in muted_users: del muted_users[uid]; request_log[uid].clear(); await update.message.reply_text(f"✅ Unmute {uid}")
        else: await update.message.reply_text("❌ Không bị mute")
async def resetall(update, context):
    if not is_admin(update.effective_user.id): return
    if not context.args or context.args[0] != "CONFIRM": await update.message.reply_text("⚠️ /resetall CONFIRM"); return
    with keys_lock: keys.clear()
    with users_lock: users.clear()
    with acc_lock: used_accounts.clear()
    with spam_lock: request_log.clear(); muted_users.clear()
    await save_data(); await update.message.reply_text("☢️ Xóa toàn bộ!")
async def error_handler(update, context): log.exception(context.error)

if __name__ == "__main__":
    Thread(target=web, daemon=True).start()
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
