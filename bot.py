import os
import asyncio
import json
import logging
import base64
from io import BytesIO
import time
import socket
from typing import Any, Dict, List, Optional

import requests
from aiotg import Bot, Chat
from aiohttp import web
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- ЛОГУВАННЯ ----------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tg-bot")

# ---------------- ОТОЧЕННЯ ----------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
API_SECRET = os.environ.get("API_SECRET")
GOOGLE_DRIVE_FILE_ID = os.environ.get("GOOGLE_DRIVE_FILE_ID")
GOOGLE_SERVICE_ACCOUNT_EMAIL = os.environ.get("GOOGLE_SERVICE_ACCOUNT_EMAIL")
GOOGLE_PRIVATE_KEY = os.environ.get("GOOGLE_PRIVATE_KEY")
PORT = int(os.environ.get("PORT", 8080))
FORCE_IPV4 = os.environ.get("FORCE_IPV4", "true").lower() in ("1", "true", "yes")

# ---------------- IPv4 ONLY ХАК ----------------
def force_ipv4_only():
    """
    На деяких хостингах (Render, Railway, ін.) egress по IPv6 може бути недоступний.
    Telegram часто резолвиться в IPv6. Хак нижче змушує socket використовувати лише IPv4.
    Можна вимкнути змінною FORCE_IPV4=false
    """
    if not FORCE_IPV4:
        logger.info("FORCE_IPV4=false — залишаємо стандартну мережеву конфігурацію.")
        return

    logger.warning("УВАГА: Вмикаю IPv4-only режим для socket.getaddrinfo (FORCE_IPV4=true).")
    _orig_getaddrinfo = socket.getaddrinfo

    def _v4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    socket.getaddrinfo = _v4_only_getaddrinfo

force_ipv4_only()

# ---------------- Google Drive ----------------
SCOPES = ['https://www.googleapis.com/auth/drive']

def _build_drive_client():
    if not all([GOOGLE_SERVICE_ACCOUNT_EMAIL, GOOGLE_PRIVATE_KEY]):
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_EMAIL та GOOGLE_PRIVATE_KEY обов'язкові.")
    creds_dict = {
        "type": "service_account",
        "project_id": GOOGLE_SERVICE_ACCOUNT_EMAIL.split('@')[1].split('.')[0],
        "private_key_id": "",
        "private_key": GOOGLE_PRIVATE_KEY.replace('\\n', '\n'),
        "client_email": GOOGLE_SERVICE_ACCOUNT_EMAIL,
        "client_id": "",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GOOGLE_SERVICE_ACCOUNT_EMAIL.replace('@', '%40')}"
    }
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

try:
    drive_service = _build_drive_client()
except Exception as e:
    logger.critical(f"Не вдалося завантажити облікові дані Google: {e}")
    raise SystemExit(1)

db_lock = asyncio.Lock()

async def read_db() -> Optional[Dict[str, Any]]:
    async with db_lock:
        try:
            request = drive_service.files().get_media(fileId=GOOGLE_DRIVE_FILE_ID)
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            raw = fh.read().decode('utf-8') if fh.getbuffer().nbytes else ""
            return json.loads(raw) if raw else {}
        except Exception as e:
            logger.error(f"Помилка читання з Google Drive: {e}")
            return None

async def write_db(data: Dict[str, Any]) -> bool:
    async with db_lock:
        try:
            fh = BytesIO(json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'))
            media = MediaIoBaseUpload(fh, mimetype='application/json', resumable=True)
            drive_service.files().update(fileId=GOOGLE_DRIVE_FILE_ID, media_body=media).execute()
            return True
        except Exception as e:
            logger.error(f"Помилка запису в Google Drive: {e}")
            return False

def find_user_by_chat_id(db_data: Dict[str, Any], chat_id: int) -> Optional[Dict[str, Any]]:
    for user in db_data.get("users", []):
        binding = user.get("telegramBinding")
        if binding and binding.get("chatId") == chat_id:
            return user
    return None

# ---------------- ІНІЦІАЛІЗАЦІЯ БОТА ----------------
if not all([BOT_TOKEN, API_SECRET, GOOGLE_DRIVE_FILE_ID, GOOGLE_SERVICE_ACCOUNT_EMAIL, GOOGLE_PRIVATE_KEY]):
    logger.critical("Критична помилка: не всі необхідні змінні середовища встановлено.")
    raise SystemExit(1)

bot = Bot(api_token=BOT_TOKEN)

# ---------------- ХЕЛПЕРИ ВІДПРАВКИ з РЕТРАЯМИ ----------------
def is_blocked_error(e: Exception) -> bool:
    msg = getattr(e, "description", "") or str(e)
    return any(s in msg for s in (
        "bot was blocked by the user",
        "user is deactivated",
        "chat not found",
        "Forbidden: bot was blocked by the user",
    ))

async def retry_send(func, *args, **kwargs):
    max_attempts = kwargs.pop("_max_attempts", 5)
    base_sleep = kwargs.pop("_base_sleep", 0.8)
    for attempt in range(1, max_attempts + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if is_blocked_error(e):
                raise
            logger.warning(f"Помилка (спроба {attempt}/{max_attempts}): {e}")
            sleep_s = base_sleep * (2 ** (attempt - 1))
            await asyncio.sleep(min(sleep_s, 10))
    raise RuntimeError("Вичерпано спроби відправки у Telegram")

# ---------------- ОБРОБНИКИ КОМАНД ----------------
@bot.command(r"/start(?:\s+(.+))?")
async def handle_start(chat: Chat, match):
    try:
        db_data = await read_db()
        if db_data is None:
            await retry_send(chat.send_text, "❌ Не вдалося прочитати БД. Спробуйте пізніше.")
            return

        chat_id = chat.id
        active_user = find_user_by_chat_id(db_data, chat_id)
        activation_id = match.group(1) if match and match.group(1) else None

        if active_user and not activation_id:
            await retry_send(chat.send_text, f"👋 Вітаю, {active_user.get('nickname', 'друже')}! Ваш акаунт вже прив'язаний та активний.")
            return

        if not activation_id:
            await retry_send(chat.send_text, "Для активації, будь ласка, використайте персональне посилання з вашого кабінету.")
            return

        if active_user:
            await retry_send(chat.send_text, "Цей Telegram акаунт вже прив'язаний до профілю. Ви не можете активувати інший код.")
            return

        user_to_activate = None
        for user in db_data.get("users", []):
            binding = user.get("telegramBinding")
            if binding and binding.get("activationId") == activation_id and binding.get("status") == "pending":
                user_to_activate = user
                break

        if user_to_activate:
            binding = user_to_activate.setdefault("telegramBinding", {})
            binding["status"] = "active"
            binding["chatId"] = chat_id
            binding["username"] = (getattr(chat, "sender", {}) or {}).get("username")

            if await write_db(db_data):
                await retry_send(chat.send_text, f"✅ Вітаю, {user_to_activate.get('nickname', '')}! Сповіщення успішно увімкнено.")
                logger.info(f"Користувач {user_to_activate.get('nickname')} (ID: {user_to_activate.get('userId')}) активував бота.")
            else:
                await retry_send(chat.send_text, "❌ Сталася помилка під час збереження даних. Спробуйте пізніше.")
        else:
            await retry_send(chat.send_text, "❌ Недійсний або вже використаний код активації.")
            logger.warning(f"Не знайдено користувача з activation_id: {activation_id}")

    except Exception as e:
        logger.error(f"Критична помилка в handle_start: {e}")

@bot.command(r".*")
async def handle_other_messages(chat: Chat, match):
    logger.info(f"Отримано непідтримуване повідомлення від {chat.id}")
    return

# ---------------- ВЕБ-СЕРВЕР ----------------
async def handle_notify(request: web.Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or auth_header != f"Bearer {API_SECRET}":
        logger.warning("Спроба неавторизованого доступу до /notify")
        return web.Response(status=401, text="Unauthorized")

    try:
        data = await request.json()
    except Exception as e:
        # Додано логування для діагностики
        logger.error(f"Помилка парсингу JSON: {e}. Розмір тіла запиту: {request.content_length} байт.")
        return web.Response(status=400, text="Bad Request: invalid or too large JSON")

    chat_id = data.get("chat_id")
    event_data = data.get("event_data")
    if not chat_id or not event_data:
        return web.Response(status=400, text="Bad Request: Missing chat_id or event_data")

    chat = Chat(bot, chat_id)
    event_type = event_data.get("type")
    
    try:
        if event_type == "photos":
            caption = (
                "📸 **Нові фото!**\n\n"
                f"**Пристрій:** `{event_data.get('fingerprint','-')}`\n"
                f"**Час:** `{event_data.get('collectedAt','-')}`"
            )
            for idx, photo_b64 in enumerate(event_data.get("data", [])[:10]):
                try:
                    payload = photo_b64.split(",", 1)[1] if "," in photo_b64 else photo_b64
                    photo_bytes = base64.b64decode(payload)
                    fh = BytesIO(photo_bytes)
                    await retry_send(
                        chat.send_photo,
                        photo=fh,
                        caption=caption if idx == 0 else None,
                        parse_mode="Markdown" if idx == 0 else None
                    )
                except Exception as e:
                    logger.error(f"Не вдалося декодувати або відправити фото #{idx}: {e}")
        
        elif event_type == "video":
            caption = (
                "📹 **Нове відео!**\n\n"
                f"**Пристрій:** `{event_data.get('fingerprint','-')}`\n"
                f"**Час:** `{event_data.get('collectedAt','-')}`"
            )
            for idx, video_b64 in enumerate(event_data.get("data", [])[:10]):
                try:
                    payload = video_b64.split(",", 1)[1] if "," in video_b64 else video_b64
                    video_bytes = base64.b64decode(payload)
                    fh = BytesIO(video_bytes)
                    fh.name = f"video_{idx+1}.webm"
                    await retry_send(
                        chat.send_video,
                        video=fh,
                        caption=caption if idx == 0 else None,
                        parse_mode="Markdown" if idx == 0 else None
                    )
                except Exception as e:
                    logger.error(f"Не вдалося декодувати або відправити відео #{idx}: {e}")

        elif event_type == "location":
            lat = event_data.get("data", {}).get("latitude")
            lon = event_data.get("data", {}).get("longitude")
            if lat is None or lon is None:
                return web.Response(status=400, text="Bad Request: location payload invalid")
            maps_link = f"https://www.google.com/maps?q={lat},{lon}"
            message_text = (
                "📍 **Отримано геолокацію!**\n\n"
                f"**Пристрій:** `{event_data.get('fingerprint','-')}`\n"
                f"**Координати:** `{lat}, {lon}`\n\n"
                f"[Відкрити на карті]({maps_link})"
            )
            await retry_send(chat.send_text, message_text, parse_mode="Markdown", disable_web_page_preview=True)

        elif event_type == "form":
            form_id = event_data.get("formId", "-")
            fields = "\n".join(
                [f"- **{key}:** `{value}`"
                 for key, value in (event_data.get("data") or {}).items()
                 if str(value).strip()]
            ) or "_(порожньо)_"
            message_text = (
                f"📝 **Заповнено форму: '{form_id}'**\n\n"
                f"**Пристрій:** `{event_data.get('fingerprint','-')}`\n\n"
                f"{fields}"
            )
            await retry_send(chat.send_text, message_text, parse_mode="Markdown")
            
        elif event_type == "device_info":
            info_items = [f"- **{key}:** `{value}`" 
                          for key, value in (event_data.get("data") or {}).items()]
            info_text = "\n".join(info_items) or "_(немає даних)_"
            message_text = (
                "ℹ️ **Інформація про пристрій**\n\n"
                f"**Відбиток (FP):** `{event_data.get('fingerprint','-')}`\n"
                f"**Час:** `{event_data.get('collectedAt','-')}`\n\n"
                f"{info_text}"
            )
            await retry_send(chat.send_text, message_text, parse_mode="Markdown")

        else:
            logger.warning(f"Отримано невідомий тип події: {event_type}. Дані: {event_data}")

    except Exception as e:
        logger.error(f"Помилка при відправці сповіщення для chat_id {chat_id}: {e}")
        if is_blocked_error(e):
            logger.warning(f"Користувач {chat_id} заблокував бота/чат. Деактивую у БД...")
            db_data = await read_db()
            if db_data:
                user_to_deactivate = find_user_by_chat_id(db_data, chat_id)
                if user_to_deactivate and user_to_deactivate.get("telegramBinding", {}).get("status") != 'bot_blocked':
                    user_to_deactivate["telegramBinding"]["status"] = 'bot_blocked'
                    await write_db(db_data)
                    
    return web.Response(status=200, text="OK")

async def handle_health_check(request: web.Request):
    return web.Response(status=200, text="OK. Bot is running.")

# ---------------- KEEP-ALIVE ----------------
async def keep_alive():
    while True:
        try:
            await bot.get_me()
            logger.info("Ping до Telegram успішний")
        except Exception as e:
            logger.warning(f"Ping до Telegram провалився: {e}")
        await asyncio.sleep(300)

# ---------------- MAIN ----------------
async def main():
    # *** ОСНОВНЕ ВИПРАВЛЕННЯ ТУТ ***
    # Збільшуємо максимальний розмір тіла запиту до 20 МБ
    app = web.Application(client_max_size=20 * 1024 * 1024)
    
    app.router.add_post("/notify", handle_notify)
    app.router.add_get("/", handle_health_check)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Веб-сервер запущено на порту {PORT}")

    asyncio.create_task(keep_alive())
    await bot.loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот зупинено.")
