import os
import re
import json
import time
import requests
from typing import Dict, List, Tuple, Any

from common.MyMQTT import MQTTClient
from common.senml import parse_senml
from common.catalog_client import CatalogClient


class BridgeSettings:
    """
    Carga settings desde JSON, sin defaults “hardcodeados”.
    Si falta algo requerido => ValueError.
    """
    REQUIRED_ROOT = [
        "catalogURL",
        "ThingspeakWriteURL",
        "brokerIP",
        "brokerPort",
        "minPeriodSec",
        "serviceInfo",
        "fields",
    ]
    REQUIRED_SERVICE = ["serviceID", "MQTT_sub"]  # MQTT_pub opcional

    def __init__(self, path: str = "settings.json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validación estricta
        for k in self.REQUIRED_ROOT:
            if k not in data:
                raise ValueError(f"[settings] Falta clave requerida: {k}")

        svc = data["serviceInfo"]
        for k in self.REQUIRED_SERVICE:
            if k not in svc:
                raise ValueError(f"[settings] Falta serviceInfo.{k}")

        if not isinstance(data["fields"], dict) or not data["fields"]:
            raise ValueError("[settings] 'fields' debe ser un objeto no vacío")

        # Asignación 1:1 desde archivo (sin defaults)
        self.catalog_url: str  = data["catalogURL"]
        self.ts_write_url: str = data["ThingspeakWriteURL"]
        self.broker_ip: str    = data["brokerIP"]
        self.broker_port: int  = int(data["brokerPort"])
        self.min_period: int   = int(data["minPeriodSec"])

        self.service_id: str      = svc["serviceID"]
        self.mqtt_subs: List[str] = list(svc["MQTT_sub"])
        self.mqtt_pub:  List[str] = list(svc.get("MQTT_pub", []))

        self.fields_map: Dict[str, str] = dict(data["fields"])

    @staticmethod
    def normalize_topics(topics: List[str]) -> List[str]:
        """
        No inventa suscripciones nuevas; solo limpia espacios y barras duplicadas.
        """
        out = []
        for t in topics:
            t = (t or "").strip()
            if not t:
                continue
            while "//" in t:
                t = t.replace("//", "/")
            out.append(t)
        return out


class ThingspeakBridge:
    """Bridge MQTT → ThingSpeak, broker+subs estrictamente desde settings.json.
       El catálogo solo se usa para obtener API keys por usuario/habitación.
    """

    RE_SC = re.compile(r"^SC/([^/]+)/([^/]+)/")  # SC/<User>/<Room>/...

    def __init__(self, settings: BridgeSettings,
                 catalog: CatalogClient | None = None,
                 mqtt_cls=MQTTClient):
        self.S = settings
        self.catalog = catalog or CatalogClient(url=self.S.catalog_url, ttl=5)
        self.mqtt_cls = mqtt_cls

        # Estado por (user,room)
        self.states: Dict[Tuple[str, str], Dict[str, Any]] = {}
        # Mapa (user,room) -> ThingSpeak write API key
        self.user_api: Dict[Tuple[str, str], str] = {}

        # Broker y subs estrictamente desde settings.json
        self.broker_host = self.S.broker_ip
        self.broker_port = self.S.broker_port
        self.subscriptions = BridgeSettings.normalize_topics(self.S.mqtt_subs)

        # Cliente MQTT
        self.mqtt = self.mqtt_cls(cid="svc-bridge-ts",
                                  host=self.broker_host,
                                  port=self.broker_port)

    # ---------- bootstrap ----------
    def _refresh_user_api_map(self):
        try:
            self.user_api = self.catalog.users_map_api_keys()
        except Exception as e:
            print("[bridge] WARN: cannot load users from catalog:", e)

    # ---------- state ----------
    def _ensure_state(self, user: str, room: str) -> Dict[str, Any]:
        key = (user, room)
        if key not in self.states:
            self.states[key] = {
                "last": 0.0,
                "vals": {
                    "temp": None, "hum": None, "bpm": None, "raw": None,
                    "servoFan": None, "servoCurtain": None, "LedL": None
                }
            }
        return self.states[key]

    @staticmethod
    def _to_bool(v):
        if isinstance(v, bool):   return v
        if isinstance(v, (int, float)): return bool(int(v))
        if isinstance(v, str):    return v.strip().lower() in ("true", "1", "on")
        return None

    # ---------- ThingSpeak POST ----------
    def _post_thingspeak(self, write_api_key: str, values: Dict[str, Any]):
        params = {"api_key": write_api_key}
        for name, field in self.S.fields_map.items():
            val = values.get(name)
            if val is None:
                continue
            if name in ("servoFan", "servoCurtain", "LedL"):
                params[field] = 1 if self._to_bool(val) else 0
            else:
                params[field] = val

        if len(params) == 1:
            # solo api_key ⇒ no hay nada que enviar
            print("[bridge] skip: no fields to send")
            return None

        print(f"[bridge] POST TS -> {params}")
        r = requests.post(self.S.ts_write_url, params=params, timeout=5)
        return r

    # ---------- MQTT callback ----------
    def _on_msg(self, topic: str, payload: str):
        # Acepta topics con o sin barra inicial (strip leading '/')
        t = topic.lstrip('/')
        m = self.RE_SC.match(t)
        if not m:
            return
        user, room = m.group(1), m.group(2)
        st = self._ensure_state(user, room)

        # Parsear SenML
        try:
            measures = parse_senml(payload)  # [(name, unit, val, ts)]
        except Exception as e:
            print("[bridge] parse error:", e, "payload=", payload[:200])
            return

        # Actualizar estado
        for name, unit, val, ts in measures:
            if not name:
                continue
            base = name.split('/')[-1]
            if base in st["vals"]:
                if base in ("servoFan", "servoCurtain", "LedL"):
                    st["vals"][base] = self._to_bool(val)
                else:
                    if isinstance(val, (int, float)):
                        st["vals"][base] = float(val)

        # Rate limit por (user,room)
        now = time.time()
        if now - st["last"] < self.S.min_period:
            return

        # API key por usuario/room desde catálogo
        api_key = self.user_api.get((user, room)) or self.user_api.get((user, "Room1"))
        if not api_key:
            # refresco on-demand
            self._refresh_user_api_map()
            api_key = self.user_api.get((user, room)) or self.user_api.get((user, "Room1"))
            if not api_key:
                print(f"[bridge] No API key for {user}/{room}, skip.")
                st["last"] = now
                return

        # Enviar a ThingSpeak
        try:
            r = self._post_thingspeak(api_key, st["vals"])
            if r is not None:
                print(f"[bridge] TS {r.status_code} ({user}/{room}) -> {st['vals']}")
            st["last"] = now
        except Exception as e:
            print("[bridge] TS error:", e)

    # ---------- run ----------
    def run(self):
        # Cargar mapa inicial de API keys
        self._refresh_user_api_map()

        # Suscripciones (estrictas desde settings)
        for t in self.subscriptions:
            self.mqtt.sub(t, self._on_msg)

        print(f"[bridge] broker={self.broker_host}:{self.broker_port} subs={self.subscriptions}")
        while True:
            time.sleep(1)


if __name__ == "__main__":
    # Ruta al settings.json (opcional por env: SETTINGS_PATH)
    settings = BridgeSettings()
    bridge = ThingspeakBridge(settings)
    bridge.run()



