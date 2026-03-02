#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TimeShift microservice

- Lee del Catálogo las horas HH:MM por usuario: user_information.timesleep / timeawake.
- Decide si ahora es "night" (ventana de sueño) o "day".
- Transición a NIGHT (bedtime):
    * Publica SC/{User}/{Room}/bedtime {"ts": epoch}           (evento efímero, retain=False)
    * sampling -> true   (SC/{User}/{Room}/sampling)           (estado, retain=True)
    * Cierra cortina     (SC/{User}/{Room}/servoV -> "0")      (estado, retain=True)
    * Apaga LED (SenML)  (SC/{User}/{Room}/LedL ...)           (estado, retain=True)
- Transición a DAY (wakeup):
    * Publica SC/{User}/{Room}/wakeup {"seconds": <n>}         (evento efímero, retain=False)
    * Decide LED según última luz (SC/{User}/{Room}/Light SenML {"v": raw}):
        - raw < umbral ⇒ LED ON, si no OFF. Umbral=(pot_min+pot_max)/2 del catálogo.
    * Abre cortina (servoV -> "90")                            (estado, retain=True)
    * sampling -> false                                        (estado, retain=True)
- Se suscribe a SC/+/+/Light para cachear última luz.

Nota: normaliza siempre IDs a {User}/{Room} para coincidir con tus topics.
"""

import os
import time
import json
import logging
import threading
from dataclasses import dataclass
from typing import Dict, Tuple, Any, List, Optional
from datetime import datetime

import requests
from paho.mqtt.client import Client as MqttClient, MQTTMessage

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - timeshift - %(levelname)s - %(message)s",
)
log = logging.getLogger("timeshift")

# --------------- Settings ---------------
@dataclass
class TSSettings:
    catalog_url: str    # e.g. http://catalog:9080
    broker_ip: str
    broker_port: int
    service_id: str

    loop_interval_sec: int = 10
    wake_alarm_seconds: int = 30
    light_threshold_fallback: int = 2048
    timezone: str = "Europe/Rome"

    @classmethod
    def load(cls, path: str = "settings.json") -> "TSSettings":
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
            service_id=si.get("serviceID", "TimeShift"),
            loop_interval_sec=int(data.get("loop_interval_sec", 10)),
            wake_alarm_seconds=int(data.get("wake_alarm_seconds", 30)),
            light_threshold_fallback=int(data.get("light_threshold_fallback", 2048)),
            timezone=data.get("timezone", "Europe/Rome"),
        )

# --------------- Catalog client ---------------
class CatalogClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")

    def catalog(self) -> Dict[str, Any]:
        r = requests.get(f"{self.base}/catalog", timeout=8)
        r.raise_for_status()
        return r.json()

    def users(self) -> List[Dict[str, Any]]:
        r = requests.get(f"{self.base}/users", timeout=8)
        r.raise_for_status()
        return r.json()

    def rooms(self) -> List[Dict[str, Any]]:
        r = requests.get(f"{self.base}/rooms", timeout=8)
        r.raise_for_status()
        return r.json()

    def get_user(self, user_id: str) -> Dict[str, Any]:
        r = requests.get(f"{self.base}/users/{user_id}", timeout=8)
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        return r.json()

# --------------- Helpers ---------------
def parse_hhmm(s: str) -> Optional[int]:
    if not s or not isinstance(s, str): return None
    s = s.strip()
    try:
        hh, mm = s.split(":")
        h = int(hh); m = int(mm)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h*60 + m
    except Exception:
        return None
    return None

def in_sleep_window(now_min: int, sleep_min: int, wake_min: int) -> bool:
    if sleep_min is None or wake_min is None:
        return False
    if sleep_min < wake_min:
        return sleep_min <= now_min < wake_min
    else:
        return now_min >= sleep_min or now_min < wake_min

def senml_led_payload(on: bool) -> str:
    return json.dumps([{
        "bn": "stateLed",
        "bt": 0,
        "e": [{"n":"LedL","u":"bool","vb": bool(on)}]
    }])

def canon_id(s: str) -> str:
    s = str(s or "")
    return s if (s.startswith("{") and s.endswith("}")) else "{"+s+"}"

# --------------- TimeShift core ---------------
class TimeShiftService:
    def __init__(self, settings: TSSettings):
        self.S = settings
        self.cat = CatalogClient(self.S.catalog_url)

        self.last_light: Dict[Tuple[str,str], int] = {}
        self.last_phase: Dict[Tuple[str,str], str] = {}

        self.light_min = 0
        self.light_max = self.S.light_threshold_fallback * 2  # ~4096
        self._load_thresholds()

        if ZoneInfo is not None:
            try:
                self.tz = ZoneInfo(self.S.timezone)
            except Exception:
                log.warning("Invalid timezone '%s', fallback to UTC", self.S.timezone)
                self.tz = ZoneInfo("UTC")
        else:
            self.tz = None

        self.mqtt = MqttClient(client_id="timeshift", clean_session=True)
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message

        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # ---------- Catalog ----------
    def _load_thresholds(self):
        try:
            cat = self.cat.catalog()
            thr = cat.get("threshold_parameters", {}) or {}
            self.light_min = int(thr.get("pot_min", 0))
            self.light_max = int(thr.get("pot_max", 4095))
            log.info("Thresholds pot_min=%s pot_max=%s", self.light_min, self.light_max)
        except Exception:
            log.exception("Error leyendo threshold_parameters del catálogo")
            self.light_min = 0
            self.light_max = 4095

    def _target_pairs(self) -> List[Tuple[str,str]]:
        pairs: List[Tuple[str,str]] = []
        try:
            users = self.cat.users()
            rooms = self.cat.rooms()

            for r in rooms:
                rid = r.get("roomID")
                uid = r.get("userID") or r.get("owner")
                if rid and uid:
                    pairs.append((str(uid), str(rid)))

            if not pairs:
                for u in users:
                    uid = u.get("userID")
                    if not uid: continue
                    for r in rooms:
                        rid = r.get("roomID")
                        if rid:
                            pairs.append((str(uid), str(rid)))

            out = []
            seen = set()
            for p in pairs:
                if p not in seen:
                    seen.add(p); out.append(p)
            return out

        except Exception:
            log.exception("No se pudieron construir pares user/room desde catálogo")
            return [("User1", "Room1")]

    def _user_times(self, user_id: str) -> Tuple[Optional[int], Optional[int]]:
        try:
            u = self.cat.get_user(user_id)
            info = u.get("user_information", {}) or {}
            ts = parse_hhmm(info.get("timesleep"))
            ta = parse_hhmm(info.get("timeawake"))
            return ts, ta
        except Exception:
            log.exception("Error leyendo times para user %s", user_id)
            return None, None

    # ---------- MQTT ----------
    def connect_mqtt(self):
        self.mqtt.connect(self.S.broker_ip, self.S.broker_port, keepalive=30)
        self._thread = threading.Thread(target=self.mqtt.loop_forever, daemon=True)
        self._thread.start()
        log.info("MQTT loop thread started.")

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.error("MQTT connect rc=%s", rc); return
        try:
            client.subscribe("SC/+/+/Light", qos=1)
            log.info("SUB SC/+/+/Light")
        except Exception:
            log.exception("subscribe Light failed")

    def on_message(self, client, userdata, msg: MQTTMessage):
        try:
            topic = msg.topic  # SC/<user>/<room>/Light
            parts = topic.split("/")
            if len(parts) == 4 and parts[0] == "SC" and parts[3] == "Light":
                user_raw, room_raw = parts[1], parts[2]
                user, room = canon_id(user_raw), canon_id(room_raw)
                raw = self._parse_light_senml(msg.payload.decode("utf-8","ignore"))
                if raw is not None:
                    self.last_light[(user,room)] = raw
        except Exception:
            log.exception("on_message error")

    @staticmethod
    def _parse_light_senml(payload: str) -> Optional[int]:
        try:
            arr = json.loads(payload)
            if isinstance(arr, list) and arr:
                rec = arr[0]
                e = rec.get("e", [])
                if isinstance(e, list):
                    for ent in e:
                        if ent.get("n") == "raw":
                            v = ent.get("v")
                            if isinstance(v, (int, float)):
                                return int(v)
        except Exception:
            return None
        return None

    # ---------- Publish helper ----------
    def _pub(self, topic: str, payload: str | bytes, *, qos: int = 1, retain: bool = False):
        try:
            res = self.mqtt.publish(topic, payload=payload, qos=qos, retain=retain)
            res.wait_for_publish()
            log.info("PUB %s (qos=%d retain=%s) -> %s", topic, qos, retain,
                     payload if isinstance(payload, str) else "<bytes>")
        except Exception:
            log.exception("Publish failed: %s", topic)

    # ---------- Publicadores ----------
    def pub_sampling(self, user: str, room: str, enable: bool):
        user, room = canon_id(user), canon_id(room)
        topic = f"SC/{user}/{room}/sampling"
        payload = json.dumps({"enable": bool(enable)})
        self._pub(topic, payload, qos=1, retain=True)   # ESTADO

    def pub_bedtime(self, user: str, room: str):
        user, room = canon_id(user), canon_id(room)
        topic = f"SC/{user}/{room}/bedtime"
        payload = json.dumps({"ts": int(time.time())})
        self._pub(topic, payload, qos=1, retain=False)  # EVENTO

    def pub_wakeup(self, user: str, room: str):
        user, room = canon_id(user), canon_id(room)
        topic = f"SC/{user}/{room}/wakeup"
        payload = json.dumps({"seconds": int(self.S.wake_alarm_seconds)})
        self._pub(topic, payload, qos=1, retain=False)  # EVENTO

    def pub_led_senml(self, user: str, room: str, on: bool):
        user, room = canon_id(user), canon_id(room)
        topic = f"SC/{user}/{room}/LedL"
        payload = senml_led_payload(on)
        self._pub(topic, payload, qos=1, retain=True)   # ESTADO

    def pub_servo(self, user: str, room: str, deg: int):
        user, room = canon_id(user), canon_id(room)
        topic = f"SC/{user}/{room}/servoV"
        payload = str(int(deg))  # "0" ó "90"
        self._pub(topic, payload, qos=1, retain=True)   # ESTADO

    # ---------- Lógica principal ----------
    def desired_phase(self, user: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
        ts, ta = self._user_times(user)
        if ts is None or ta is None:
            return None, ts, ta
        now = datetime.now(self.tz) if self.tz is not None else datetime.now()
        now_min = now.hour*60 + now.minute
        night = in_sleep_window(now_min, ts, ta)
        return ("night" if night else "day"), ts, ta

    def light_needs_led(self, user: str, room: str) -> bool:
        user, room = canon_id(user), canon_id(room)
        raw = self.last_light.get((user, room))
        if raw is None:
            log.info("No light cached for %s/%s -> LED ON by default", user, room)
            return True
        thr = (self.light_min + self.light_max) / 2.0
        need = raw < thr
        log.info("Light %s/%s raw=%s thr=%.1f -> LED %s", user, room, raw, thr, "ON" if need else "OFF")
        return need

    def do_bedtime(self, user: str, room: str):
        self.pub_bedtime(user, room)          # evento
        self.pub_sampling(user, room, True)   # estado
        self.pub_servo(user, room, 0)         # estado
        self.pub_led_senml(user, room, False) # estado

    def do_wakeup(self, user: str, room: str):
        self.pub_wakeup(user, room)           # evento
        led_on = self.light_needs_led(user, room)
        self.pub_led_senml(user, room, led_on) # estado
        self.pub_servo(user, room, 90)         # estado
        self.pub_sampling(user, room, False)   # estado

    def run(self):
        self.connect_mqtt()
        log.info("TimeShift running every %ss (TZ=%s)", self.S.loop_interval_sec, self.S.timezone)

        while not self._stop.is_set():
            try:
                pairs = self._target_pairs()
                for (user_raw, room_raw) in pairs:
                    user, room = canon_id(user_raw), canon_id(room_raw)
                    phase, ts, ta = self.desired_phase(user_raw)
                    if phase is None:
                        continue

                    key = (user, room)
                    last = self.last_phase.get(key)

                    if last != phase:
                        self.last_phase[key] = phase
                        if phase == "night":
                            log.info("[%s/%s] Transition -> NIGHT", user, room)
                            self.do_bedtime(user, room)
                        else:
                            log.info("[%s/%s] Transition -> DAY", user, room)
                            self.do_wakeup(user, room)

                self._stop.wait(self.S.loop_interval_sec)

            except Exception:
                log.exception("loop error")
                self._stop.wait(self.S.loop_interval_sec)

    def stop(self):
        self._stop.set()
        try:
            self.mqtt.disconnect()
        except Exception:
            pass

# --------------- Bootstrap ---------------
def main():
    S = TSSettings.load("settings.json")
    svc = TimeShiftService(S)
    try:
        svc.run()
    except KeyboardInterrupt:
        svc.stop()

if __name__ == "__main__":
    main()
