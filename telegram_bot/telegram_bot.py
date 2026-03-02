#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, Set, List

import requests
from paho.mqtt.client import Client as MqttClient, MQTTMessage

from telegram import (
    Update,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - telegrambot - %(levelname)s - %(message)s",
)
log = logging.getLogger("telegrambot")

# ---------------- Settings ----------------
@dataclass
class BotSettings:
    catalog_url: str        # base, e.g. http://catalog:9080
    broker_ip: str          # MQTT host
    broker_port: int        # 1883
    service_id: str         # TelegramBot
    telegram_token: str     # BotFather token
    mqtt_subs: List[str]    # explicit subs from settings.json

    @classmethod
    def load(cls, path: str = "settings.json") -> "BotSettings":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        si = data["serviceInfo"]
        base = data["catalogURL"].rstrip("/")
        if base.endswith("/catalog"):
            base = base[: -len("/catalog")]
        return cls(
            catalog_url=base,
            broker_ip=data["brokerIP"],
            broker_port=int(data["brokerPort"]),
            service_id=si["serviceID"],
            telegram_token=si["telegram_token"],
            mqtt_subs=list(si.get("MQTT_sub", [])),
        )

# ---------------- Catalog client ----------------
class CatalogAPI:
    def __init__(self, base_url: str, write_token: Optional[str] = None):
        self.base = base_url.rstrip("/")
        self.headers = {"Content-Type": "application/json"}
        if write_token:
            self.headers["X-Write-Token"] = write_token

    def get_catalog(self) -> Dict[str, Any]:
        r = requests.get(f"{self.base}/catalog", timeout=6)
        r.raise_for_status()
        return r.json()

    def get_user(self, user_id: str) -> Dict[str, Any]:
        r = requests.get(f"{self.base}/users/{user_id}", timeout=6)
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        return r.json()

    def patch_user(self, user_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        r = requests.patch(
            f"{self.base}/users/{user_id}",
            data=json.dumps(patch),
            headers=self.headers,
            timeout=8,
        )
        r.raise_for_status()
        return r.json()

    def find_user_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        cat = self.get_catalog()
        for u in cat.get("usersList", []):
            info = u.get("user_information", {}) or {}
            if str(info.get("phone", "")).strip() == phone.strip():
                return u
        return None

# ---------------- Conversation states ----------------
ASK_PHONE, MAIN_MENU, CFG_MENU, CFG_TIME_AWAKE, CFG_TIME_SLEEP, CFG_TEMP_LOW, CFG_TEMP_HIGH, CFG_HUM_LOW, CFG_HUM_HIGH = range(9)

MAIN_KB = ReplyKeyboardMarkup(
    [["1. Configuration", "2. Show dashboard"]],
    resize_keyboard=True,
)

CFG_KB = ReplyKeyboardMarkup(
    [["1. Wake/Sleep time"],
     ["2. Temp/Humidity min-max"],
     ["⬅️ Back"]],
    resize_keyboard=True,
)

# ---------------- Utilities ----------------
PHONE_RE = re.compile(r"^\+?\d{7,15}$")
TIME_RE  = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
NUM_RE   = re.compile(r"^-?\d+(\.\d+)?$")

def ok_num(s: str) -> bool:
    return bool(NUM_RE.match((s or "").strip()))

def ok_time(s: str) -> bool:
    return bool(TIME_RE.match((s or "").strip()))

# ---------------- TelegramBot Service ----------------
class TelegramBotService:
    """
    - Telegram UX (English text)
    - Verify identity by phone stored in Catalog
    - Configure times & thresholds (writes back to Catalog)
    - Track verified sessions to route MQTT alerts and bedtime/wakeup notifications
    """
    def __init__(self, settings: BotSettings):
        self.S = settings
        self.cat = CatalogAPI(self.S.catalog_url)
        # chat_id -> user_id
        self.session_by_chat: Dict[int, str] = {}
        # user_id -> set(chat_id)
        self.chats_by_user: Dict[str, Set[int]] = {}
        # temp data per chat
        self.tmp: Dict[int, Dict[str, Any]] = {}
        # PTB application (set in build_app)
        self.application = None  # type: ignore

    # ---- Telegram Handlers ----
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        self.tmp.pop(chat_id, None)
        await update.message.reply_text(
            "👋 Hi! I’m your sleep monitoring assistant.\n\n"
            "Please verify your identity by sending your *phone number* "
            "(international format, e.g. `+573001112233`).\n"
            "The number must already exist in the Catalog.",
            parse_mode="Markdown",
        )
        return ASK_PHONE

    async def ask_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        phone = (update.message.text or "").strip()
        if not PHONE_RE.match(phone):
            await update.message.reply_text("❌ Invalid phone format. Try `+573001112233`.")
            return ASK_PHONE

        try:
            user = self.cat.find_user_by_phone(phone)
        except Exception:
            log.exception("Catalog error on find_user_by_phone")
            await update.message.reply_text("⚠️ Catalog lookup error. Please try again later.")
            return ASK_PHONE

        if not user:
            await update.message.reply_text("❌ Phone not found in Catalog. Check it and try again.")
            return ASK_PHONE

        user_id = user.get("userID")
        uname = user.get("user_information", {}).get("userName", user_id)

        chat_id = update.effective_chat.id
        self.session_by_chat[chat_id] = user_id
        self.chats_by_user.setdefault(user_id, set()).add(chat_id)

        await update.message.reply_text(
            f"✅ Verified for *{uname}* (`{user_id}`). Choose an option:",
            parse_mode="Markdown",
            reply_markup=MAIN_KB,
        )
        return MAIN_MENU

    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        txt = (update.message.text or "").strip().lower()
        if txt.startswith("1"):
            await update.message.reply_text("⚙️ Configuration:", reply_markup=CFG_KB)
            return CFG_MENU
        elif txt.startswith("2"):
            chat_id = update.effective_chat.id
            user_id = self.session_by_chat.get(chat_id)
            if not user_id:
                await update.message.reply_text("⚠️ Session not verified. Use /start.")
                return ASK_PHONE
            try:
                user = self.cat.get_user(user_id)
                channel = user.get("thingspeak_info", {}).get("channel")
                if not channel:
                    await update.message.reply_text("⚠️ No ThingSpeak channel in Catalog.")
                    return MAIN_MENU
                url = f"https://thingspeak.com/channels/{channel}"
                await update.message.reply_text(f"📊 Your dashboard: {url}")
            except Exception:
                await update.message.reply_text("⚠️ Catalog error.")
            return MAIN_MENU
        else:
            await update.message.reply_text("Please pick one option from the menu.", reply_markup=MAIN_KB)
            return MAIN_MENU

    async def cfg_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        txt = (update.message.text or "").strip().lower()
        if txt.startswith("1"):
            await update.message.reply_text("⏰ Send *wake-up time* in HH:MM (24h).", parse_mode="Markdown")
            return CFG_TIME_AWAKE
        elif txt.startswith("2"):
            await update.message.reply_text("🌡️ Send *minimum temperature (°C)*:")
            return CFG_TEMP_LOW
        elif "back" in txt:
            await update.message.reply_text("Main menu:", reply_markup=MAIN_KB)
            return MAIN_MENU
        else:
            await update.message.reply_text("Choose one option from configuration menu.", reply_markup=CFG_KB)
            return CFG_MENU

    # ----- Times -----
    async def set_time_awake(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        s = (update.message.text or "").strip()
        if not ok_time(s):
            await update.message.reply_text("❌ Invalid time. Example: `06:30`")
            return CFG_TIME_AWAKE
        self.tmp.setdefault(chat_id, {})["timeawake"] = s
        await update.message.reply_text("Now send *sleep time* (HH:MM).", parse_mode="Markdown")
        return CFG_TIME_SLEEP

    async def set_time_sleep(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        s = (update.message.text or "").strip()
        if not ok_time(s):
            await update.message.reply_text("❌ Invalid time. Example: `22:45`")
            return CFG_TIME_SLEEP

        user_id = self.session_by_chat.get(chat_id)
        if not user_id:
            await update.message.reply_text("⚠️ Session not verified. Use /start.")
            return ASK_PHONE

        times = self.tmp.setdefault(chat_id, {})
        times["timesleep"] = s

        try:
            user = self.cat.get_user(user_id)
            info = user.get("user_information", {}) or {}
            info["timeawake"] = times.get("timeawake")
            info["timesleep"] = times.get("timesleep")
            self.cat.patch_user(user_id, {"user_information": info})
            await update.message.reply_text("✅ Times updated in Catalog.", reply_markup=CFG_KB)
        except Exception:
            log.exception("patch_user times")
            await update.message.reply_text("⚠️ Error saving to Catalog.")
        return CFG_MENU

    # ----- Thresholds -----
    async def set_temp_low(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        s = (update.message.text or "").strip()
        if not ok_num(s):
            await update.message.reply_text("❌ Invalid number. Example: 18.0")
            return CFG_TEMP_LOW
        self.tmp.setdefault(chat_id, {})["temp_low"] = float(s)
        await update.message.reply_text("Now send *maximum temperature (°C)*:")
        return CFG_TEMP_HIGH

    async def set_temp_high(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        s = (update.message.text or "").strip()
        if not ok_num(s):
            await update.message.reply_text("❌ Invalid number. Example: 25.0")
            return CFG_TEMP_HIGH
        self.tmp.setdefault(chat_id, {})["temp_high"] = float(s)
        await update.message.reply_text("Now send *minimum humidity (%)*:")
        return CFG_HUM_LOW

    async def set_hum_low(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        s = (update.message.text or "").strip()
        if not ok_num(s):
            await update.message.reply_text("❌ Invalid number. Example: 35")
            return CFG_HUM_LOW
        self.tmp.setdefault(chat_id, {})["hum_low"] = float(s)
        await update.message.reply_text("Finally, send *maximum humidity (%)*:")
        return CFG_HUM_HIGH

    async def set_hum_high(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        s = (update.message.text or "").strip()
        if not ok_num(s):
            await update.message.reply_text("❌ Invalid number. Example: 60")
            return CFG_HUM_HIGH

        vals = self.tmp.setdefault(chat_id, {})
        vals["hum_high"] = float(s)

        user_id = self.session_by_chat.get(chat_id)
        if not user_id:
            await update.message.reply_text("⚠️ Session not verified. Use /start.")
            return ASK_PHONE

        try:
            user = self.cat.get_user(user_id)
            thr = user.get("threshold_parameters", {}) or {}
            thr.update({
                "temp_low": vals["temp_low"],
                "temp_high": vals["temp_high"],
                "hum_low":  vals["hum_low"],
                "hum_high": vals["hum_high"],
            })
            self.cat.patch_user(user_id, {"threshold_parameters": thr})
            await update.message.reply_text("✅ Temp/Humidity thresholds updated in Catalog.", reply_markup=CFG_KB)
        except Exception:
            log.exception("patch_user thresholds")
            await update.message.reply_text("⚠️ Error saving to Catalog.")
        return CFG_MENU

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("👋 Bye. Use /start anytime.")
        return ConversationHandler.END

# ---------------- MQTT Alerts + TimeShift Listener ----------------
# ---------------- MQTT Alerts Listener ----------------
class AlertsMQTT:
    """
    Background MQTT listener that subscribes to alerts *and* bedtime/wakeup topics
    and forwards messages to verified chats.

    - Alerts:
        * send on transition to ALERT (previous != ALERT)
        * and every 120s while staying in ALERT
        * never send for OK
    - Bedtime/Wakeup:
        * send once whenever a message arrives (TimeShift publishes only on transition)
    """
    RESEND_SECONDS = 120  # 2 minutes

    def __init__(self, svc):
        self.svc = svc
        self.host = svc.S.broker_ip
        self.port = svc.S.broker_port
        self.subs = svc.S.mqtt_subs or []
        self.client = MqttClient(client_id="telegram-bot-alerts", clean_session=True)
        self.thread: Optional[threading.Thread] = None

        # key: (user, room, leaf) -> {"last_status": str|None, "last_sent": float|0}
        # Used for alerts only.
        self.state: Dict[tuple, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _normalized_subs(self) -> List[str]:
        out = set(self.subs)
        # Alerts (env, hr, etc.)
        out.add("SC/alerts/+/+/#")
        # Sleep/wake control events from TimeShift
        out.add("SC/+/+/bedtime")
        out.add("SC/+/+/wakeup")
        return list(out)

    # ---- MQTT callbacks ----
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("MQTT connected to %s:%s", self.host, self.port)
            for t in self._normalized_subs():
                try:
                    client.subscribe(t, qos=1)
                    log.info("MQTT SUB %s", t)
                except Exception:
                    log.exception("MQTT subscribe failed: %s", t)
        else:
            log.error("MQTT connection failed rc=%s", rc)

    def on_message(self, client, userdata, msg: MQTTMessage):
        try:
            topic = msg.topic  # e.g. SC/alerts/{User1}/{Room1}/hr  OR  SC/{User1}/{Room1}/bedtime
            payload = msg.payload.decode("utf-8", errors="ignore")
            parts = topic.split("/")

            if len(parts) < 3 or parts[0] != "SC":
                return

            # --- Bedtime / Wakeup handling ---
            if len(parts) == 4 and parts[2] and parts[3] in ("bedtime", "wakeup"):
                user_id, room_id, leaf = parts[1], parts[2], parts[3]
                chats = self.svc.chats_by_user.get(user_id, set())
                if not chats:
                    return
                text = self._format_sleep_text(leaf, user_id, room_id)
                for chat_id in list(chats):
                    self._send_to_chat_sync(chat_id, text)
                return

            # --- Alerts handling (previous logic) ---
            if len(parts) >= 5 and parts[1] == "alerts":
                user_id, room_id, leaf = parts[2], parts[3], parts[4]
                chats = self.svc.chats_by_user.get(user_id, set())
                if not chats:
                    return

                status = self._extract_status(leaf, payload)
                if status is None:
                    return

                key = (user_id, room_id, leaf)
                now = time.time()

                with self._lock:
                    st = self.state.get(key, {"last_status": None, "last_sent": 0.0})
                    last_status = st.get("last_status")
                    last_sent = float(st.get("last_sent") or 0.0)

                    should_send = False
                    if status == "ALERT":
                        if last_status != "ALERT":
                            should_send = True
                        elif (now - last_sent) >= self.RESEND_SECONDS:
                            should_send = True
                    else:
                        should_send = False

                    st["last_status"] = status
                    if should_send:
                        st["last_sent"] = now
                    self.state[key] = st

                if should_send:
                    text = self._format_alert_text(leaf, payload, topic, user_id, room_id)
                    if not text:
                        return
                    for chat_id in list(chats):
                        self._send_to_chat_sync(chat_id, text)

        except Exception:
            log.exception("on_message error")

    # ---- Helpers ----
    @staticmethod
    def _extract_status(leaf: str, payload: str) -> Optional[str]:
        """
        Extract status from payload. Returns "ALERT", "OK", or None if unknown.
        (Same as before)
        """
        try:
            obj = json.loads(payload)
        except Exception:
            obj = None

        if leaf == "hr":
            if isinstance(obj, dict):
                status = obj.get("status") or (obj.get("event") or {}).get("status")
                if isinstance(status, str):
                    status_up = status.strip().upper()
                    if status_up in ("ALERT", "OK"):
                        return status_up
            if '"status":"ALERT"' in payload:
                return "ALERT"
            if '"status":"OK"' in payload:
                return "OK"
            return None

        if leaf == "dht":
            if isinstance(obj, dict):
                evs = obj.get("events", [])
                saw_alert = False
                saw_ok = False
                for e in evs:
                    s = e.get("status")
                    if isinstance(s, str):
                        su = s.strip().upper()
                        if su == "ALERT":
                            saw_alert = True
                        elif su == "OK":
                            saw_ok = True
                if saw_alert:
                    return "ALERT"
                if saw_ok and not saw_alert:
                    return "OK"
            if '"status":"ALERT"' in payload:
                return "ALERT"
            if '"status":"OK"' in payload:
                return "OK"
            return None

        if isinstance(obj, dict):
            s = obj.get("status") or (obj.get("event") or {}).get("status")
            if isinstance(s, str):
                su = s.strip().upper()
                if su in ("ALERT", "OK"):
                    return su
        if '"status":"ALERT"' in payload:
            return "ALERT"
        if '"status":"OK"' in payload:
            return "OK"
        return None

    @staticmethod
    def _format_alert_text(leaf: str, payload: str, topic: str, user: str, room: str) -> str:
        try:
            obj = json.loads(payload)
        except Exception:
            obj = None

        if leaf == "hr":
            if isinstance(obj, dict):
                var = obj.get("variable", "bpm")
                val = obj.get("value")
                status = obj.get("status")
                bounds = obj.get("bounds", [])
                msg = obj.get("message", "")
                return (
                    f"❤️ Heart Rate Alert [{user}/{room}]\n"
                    f"Status: {status}\n"
                    f"{var.upper()}: {val}\n"
                    f"Range: {bounds}\n"
                    f"{msg}"
                )
            return f"❤️ Heart Rate alert [{user}/{room}] on topic {topic}:\n{payload}"

        if leaf == "dht":
            if isinstance(obj, dict):
                events = obj.get("events", [])
                lines = []
                for e in events:
                    var = e.get("variable")
                    val = e.get("value")
                    status = e.get("status")
                    bounds = e.get("bounds", [])
                    lines.append(f"- {var}: {val}  |  Status: {status}  |  Range: {bounds}")
                head = f"🌡️ Environment Alert [{user}/{room}]"
                return head + "\n" + "\n".join(lines) if lines else head
            return f"🌡️ Environment alert [{user}/{room}] on topic {topic}:\n{payload}"

        if isinstance(obj, dict):
            status = obj.get("status") or (obj.get("event") or {}).get("status")
            return f"🚨 Alert [{user}/{room}] ({leaf}) — Status: {status}\n{json.dumps(obj, ensure_ascii=False)}"
        return f"🚨 Alert [{user}/{room}] ({leaf})\n{payload}"

    @staticmethod
    def _format_sleep_text(leaf: str, user: str, room: str) -> str:
        """Friendly English messages for bedtime/wakeup."""
        if leaf == "bedtime":
            return ("😴 It's time to sleep.\n"
                    "Please get ready. From now on, sensors and sleep monitoring are active.\n"
                    "Have a good night! 🌙")
        # wakeup
        return ("⏰ Time to wake up!\n"
                "Monitoring has been deactivated. Check your dashboard for stats and analysis of your sleep.\n"
                "Have a great day! ☀️")

    def _send_to_chat_sync(self, chat_id: int, text: str):
        """Send a Telegram message synchronously from the MQTT thread (safe & simple)."""
        try:
            url = f"https://api.telegram.org/bot{self.svc.S.telegram_token}/sendMessage"
            r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=8)
            if r.status_code != 200:
                log.error("Telegram sendMessage failed %s: %s", r.status_code, r.text)
        except Exception:
            log.exception("Telegram sendMessage request error")

    def start(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.host, self.port, keepalive=30)
        self.thread = threading.Thread(target=self.client.loop_forever, daemon=True)
        self.thread.start()
        log.info("MQTT loop thread started.")

# ---------------- Bootstrap ----------------
def build_app(bot: TelegramBotService):
    app = ApplicationBuilder().token(bot.S.telegram_token).build()
    bot.application = app  # expose to MQTT listener

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", bot.start)],
        states={
            ASK_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.ask_phone)],
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.main_menu)],
            CFG_MENU:  [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.cfg_menu)],
            CFG_TIME_AWAKE: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.set_time_awake)],
            CFG_TIME_SLEEP: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.set_time_sleep)],
            CFG_TEMP_LOW:   [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.set_temp_low)],
            CFG_TEMP_HIGH:  [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.set_temp_high)],
            CFG_HUM_LOW:    [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.set_hum_low)],
            CFG_HUM_HIGH:   [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.set_hum_high)],
        },
        fallbacks=[CommandHandler("cancel", bot.cancel)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    return app

if __name__ == "__main__":
    S = BotSettings.load("settings.json")
    service = TelegramBotService(S)
    application = build_app(service)

    alerts = AlertsMQTT(service)
    alerts.start()

    log.info("TelegramBot started. Listening for alerts, bedtime/wakeup and user commands.")
    application.run_polling(close_loop=False)

