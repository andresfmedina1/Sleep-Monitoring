import json
import time
from typing import Dict, Any, List, Tuple
from common.MyMQTT import MQTTClient
from common.senml import parse_senml
from common.catalog_client import CatalogClient


class AlarmSettings:
    def __init__(self, path: str = "settings.json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.catalog_url: str = data["catalogURL"]
        self.broker_ip: str = data["brokerIP"]
        self.broker_port: int = int(data["brokerPort"])
        svc = data["serviceInfo"]
        self.service_id: str = svc["serviceID"]
        self.subscriptions: List[str] = list(svc["MQTT_sub"])
        self.from_catalog: bool = bool(data.get("fromCatalog", True))
        self.thresholds: Dict[str, Tuple[float, float]] = data["thresholds"]
        self.pub_alert_env: str = svc["MQTT_pub_alert_env"]
        self.pub_alert_hr:  str = svc["MQTT_pub_alert_hr"]



class AlarmControl:
    """Microservicio que evalúa HR, temperatura y humedad."""

    RE_SC = r"^SC/([^/]+)/([^/]+)/"

    def __init__(self, settings: AlarmSettings):
        self.S = settings
        self.catalog = CatalogClient(url=self.S.catalog_url, ttl=5)
        self.mqtt = MQTTClient(
            cid=f"svc-{self.S.service_id}",
            host=self.S.broker_ip,
            port=self.S.broker_port,
        )

    # ---------- Evaluación de reglas ----------
    def _check_limits(self, var: str, value: float) -> Dict[str, Any] | None:
        thr = self.S.thresholds.get(var)
        if not thr or value is None:
            return None

        low, high = thr
        if value < low or value > high:
            return {
                "variable": var,
                "value": value,
                "severity": "warning",
                "message": f"{var} out of range ({low}-{high})",
            }
        return None

    # ---------- Publicación ----------
    def _publish_alert_env(self, user: str, room: str, src_topic: str, payload: Dict[str, Any]):
        msg = {
            "service": self.S.service_id,
            "source_topic": src_topic,
            "type": "env",
            **payload,
            "ts": int(time.time())
        }
        topic = f"{self.S.pub_alert_env}".replace("{User1}", user).replace("{Room1}", room)
        self.mqtt.pub(topic, json.dumps(msg), qos=1, retain=False)
        print(f"[alarm] ALERT ENV -> {topic}: {payload}")

    def _publish_alert_hr(self, user: str, room: str, src_topic: str, payload: Dict[str, Any]):
        msg = {
            "service": self.S.service_id,
            "source_topic": src_topic,
            "type": "hr",
            **payload,
            "ts": int(time.time())
        }
        topic = f"{self.S.pub_alert_hr}".replace("{User1}", user).replace("{Room1}", room)
        self.mqtt.pub(topic, json.dumps(msg), qos=1, retain=False)
        print(f"[alarm] ALERT HR  -> {topic}: {payload}")


        # ---------- Callback MQTT ----------
    def _on_msg(self, topic: str, payload: str):
        t = topic.lstrip("/")           # tolera "/SC" o "SC"
        parts = t.split("/")
        if len(parts) < 4 or parts[0] != "SC":
            return
        user, room, leaf = parts[1], parts[2], parts[3]  # leaf: hr | dht

        # Parseo SenML robusto
        try:
            measures = parse_senml(payload)
        except Exception as e:
            print(f"[alarm] bad SenML: {e}")
            return

        vals: Dict[str, float] = {}
        for name, unit, val, ts in measures:
            name = name.replace("//", "/")
            base = name.split("/")[-1]
            if base in ("bpm", "temp", "hum"):
                try:
                    vals[base] = float(val)
                except Exception:
                    pass

        if leaf == "hr":
            # ----- HR en tópico dedicado -----
            v = vals.get("bpm")
            if v is None:
                return

            low, high = self.S.thresholds["bpm"]
            in_range = (low <= v <= high)

            self._publish_alert_hr(user, room, t, {
                "variable": "bpm",
                "value": v,
                "bounds": [low, high],
                "status": "OK" if in_range else "ALERT",
                "message": (
                    "bpm within range"
                    if in_range
                    else f"bpm out of range ({low}-{high})"
                ),
            })
            return


        if leaf == "dht":
            # ----- TEMP+HUM: publicar SIEMPRE un único mensaje con ambas -----
            temp = vals.get("temp")
            hum  = vals.get("hum")

            # Si no llegó ninguno, no publicamos
            if temp is None and hum is None:
                return

            def pack(var: str, val):
                low, high = self.S.thresholds[var]
                if val is None:
                    return {
                        "variable": var,
                        "value": None,
                        "bounds": [low, high],
                        "status": "NODATA",
                        "message": "no value"
                    }
                in_range = (low <= val <= high)
                return {
                    "variable": var,
                    "value": val,
                    "bounds": [low, high],
                    "status": "OK" if in_range else "ALERT",
                    "message": ("within range"
                                if in_range
                                else f"out of range ({low}-{high})")
                }

            payload = {
                "events": [
                    pack("temp", temp),
                    pack("hum",  hum)
                ]
            }
            self._publish_alert_env(user, room, t, payload)
            return



    # ---------- Ciclo ----------
    def run(self):
        for t in self.S.subscriptions:
            self.mqtt.sub(t, self._on_msg)
        print(f"[alarm] broker={self.S.broker_ip}:{self.S.broker_port} subs={self.S.subscriptions}")
        while True:
            time.sleep(1)


if __name__ == "__main__":
    st = AlarmSettings("settings.json")
    svc = AlarmControl(st)
    svc.run()
