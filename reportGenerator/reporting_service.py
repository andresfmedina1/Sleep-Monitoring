import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple, List

import cherrypy
import requests
import pandas as pd
import numpy as np
from dateutil import tz, parser as dateparser


# ============================ Utilidades de tiempo ============================

def now_rome() -> datetime:
    return datetime.now(tz.gettz("Europe/Rome"))


# ============================= Catálogo / Usuarios ============================

def update_catalog_last_updated(catalog_url: str, service_id: str, rest_endpoint: str) -> None:
    """Notifica al catálogo que este servicio está vivo (best-effort)."""
    try:
        payload = {
            "serviceID": service_id,
            "REST_endpoint": rest_endpoint,
            "last_updated": now_rome().strftime("%Y-%m-%d %H:%M:%S")
        }
        requests.post(f"{catalog_url}/services/{service_id}", json=payload, timeout=5)
    except requests.RequestException:
        pass


def _find_user_in_catalog_root(doc: Dict[str, Any], user_id: str) -> Optional[Dict[str, Any]]:
    users = []
    if isinstance(doc, dict):
        users = doc.get("usersList") or []
    if not users:
        return None

    wanted = {user_id, f"{{{user_id}}}"}  # soporta "User1" y "{User1}"
    for u in users:
        uid = u.get("userID")
        uname = (u.get("user_information") or {}).get("userName")
        if uid in wanted or uname == user_id:
            return u
    return None


def get_user_from_catalog(catalog_url: str, user_id: str) -> Dict[str, Any]:
    """
    Soporta dos layouts: (A) /users/{id} devuelve el usuario;
    (B) raíz (/ o /catalog o /api/catalog) con usersList[].
    Acepta IDs con y sin llaves.
    """
    # 1) Intento directo
    try:
        r = requests.get(f"{catalog_url}/users/{user_id}", timeout=8)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and ("user_information" in data or "thingspeak_info" in data):
                return data
        elif r.status_code != 404:
            r.raise_for_status()
    except requests.RequestException:
        pass

    # 2) Fallbacks al documento principal
    for suffix in ("", "/catalog", "/api/catalog"):
        try:
            r2 = requests.get(f"{catalog_url}{suffix}", timeout=8)
            r2.raise_for_status()
            doc = r2.json()
            found = _find_user_in_catalog_root(doc, user_id)
            if found:
                return found
        except requests.RequestException:
            continue

    raise cherrypy.HTTPError(404, f"User '{user_id}' not found in catalog")


def extract_times(user_obj: Dict[str, Any]) -> Dict[str, str]:
    ui = user_obj.get("user_information", {}) or {}
    # Fallback si vinieran planos en el objeto
    timesleep = ui.get("timesleep") or user_obj.get("timesleep")
    timeawake = ui.get("timeawake") or user_obj.get("timeawake")
    if not timesleep or not timeawake:
        raise cherrypy.HTTPError(400, "timesleep/timeawake missing in catalog user object")
    return {"timesleep": str(timesleep), "timeawake": str(timeawake)}


def extract_thingspeak(user_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Devuelve channel y la lista de keys (posibles READ/WRITE) tal cual.
    Probaremos todas como READ por si el orden varía.
    """
    tsi = user_obj.get("thingspeak_info", {}) or {}
    channel = str(tsi.get("channel") or "").strip()
    apikeys = tsi.get("apikeys") or []

    keys: List[str] = []
    if isinstance(apikeys, list):
        keys = [str(k).strip() for k in apikeys if k]

    return {"channel": channel, "keys": keys}


# ============================== Ventana de sueño ==============================

def window_for_date(timesleep: str, timeawake: str, ref_date_rome: datetime) -> Tuple[datetime, datetime]:
    """
    Devuelve la ventana [start, end) en Europe/Rome:
    - timesleep < timeawake  -> misma fecha (ej. 16:00–17:00)
    - timesleep >= timeawake -> cruza medianoche (ej. 22:00–07:00)
    """
    tz_rome = tz.gettz("Europe/Rome")
    today = ref_date_rome.astimezone(tz_rome).date()
    yesterday = today - timedelta(days=1)

    ts_h, ts_m = map(int, timesleep.split(":"))
    ta_h, ta_m = map(int, timeawake.split(":"))

    ts_today = datetime(today.year, today.month, today.day, ts_h, ts_m, tzinfo=tz_rome)
    ta_today = datetime(today.year, today.month, today.day, ta_h, ta_m, tzinfo=tz_rome)

    if ts_today < ta_today:
        start_dt = ts_today
        end_dt = ta_today
    else:
        start_dt = datetime(yesterday.year, yesterday.month, yesterday.day, ts_h, ts_m, tzinfo=tz_rome)
        end_dt = ta_today

    return start_dt, end_dt


# ================================ ThingSpeak ==================================

def _normalize_ts_df(feeds: list) -> pd.DataFrame:
    """
    Convierte feeds de TS a DataFrame y normaliza created_at a Europe/Rome.
    """
    if not feeds:
        return pd.DataFrame(columns=["created_at"])
    df = pd.DataFrame(feeds)
    if "created_at" in df.columns:
        # TS devuelve UTC con 'Z'; si viniera naive, igual utc=True funciona
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
        df["created_at"] = df["created_at"].dt.tz_convert("Europe/Rome")
    return df


def fetch_ts_robusto(base_url: str, channel_id: str, keys: List[str],
                     start_local: datetime, end_local: datetime) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Intenta varias combinaciones hasta obtener feeds:
      A) timezone=Europe/Rome con cada key y sin key (si canal es público)
      B) start/end en UTC (sin timezone) con cada key y sin key
    Devuelve (df_normalizado, debug_info).
    """
    def _do_req(params: dict) -> dict:
        url = f"{base_url}/channels/{channel_id}/feeds.json"
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        return r.json() if r.content else {}

    debug = {"attempts": []}

    # 1) A: pasar ventana local con timezone
    candidate_keys = keys + [None] if keys else [None]
    for key in candidate_keys:
        params = {
            "start": start_local.strftime("%Y-%m-%d %H:%M:%S"),
            "end":   end_local.strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "Europe/Rome",
        }
        if key:
            params["api_key"] = key
        try:
            obj = _do_req(params)
            feeds = obj.get("feeds", []) if isinstance(obj, dict) else []
            debug["attempts"].append({
                "mode": "local_tz",
                "key_tail": (key[-4:] if key else None),
                "feeds": len(feeds)
            })
            if feeds:
                return _normalize_ts_df(feeds), debug
        except requests.RequestException as e:
            debug["attempts"].append({
                "mode": "local_tz",
                "key_tail": (key[-4:] if key else None),
                "error": str(e)
            })

    # 2) B: convertir a UTC y NO enviar timezone
    start_utc = start_local.astimezone(timezone.utc)
    end_utc   = end_local.astimezone(timezone.utc)
    for key in candidate_keys:
        params = {
            "start": start_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "end":   end_utc.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if key:
            params["api_key"] = key
        try:
            obj = _do_req(params)
            feeds = obj.get("feeds", []) if isinstance(obj, dict) else []
            debug["attempts"].append({
                "mode": "utc_no_tzparam",
                "key_tail": (key[-4:] if key else None),
                "feeds": len(feeds)
            })
            if feeds:
                return _normalize_ts_df(feeds), debug
        except requests.RequestException as e:
            debug["attempts"].append({
                "mode": "utc_no_tzparam",
                "key_tail": (key[-4:] if key else None),
                "error": str(e)
            })

    # Nada funcionó
    return pd.DataFrame(columns=["created_at"]), debug


# ============================== Métricas/estadísticos ==============================

def pick_fields(df: pd.DataFrame, bpm_field: str, temp_field: str,
                hum_field: str, led_field: str) -> pd.DataFrame:
    cols = ["created_at", bpm_field, temp_field, hum_field, led_field]
    keep = [c for c in cols if c in df.columns]
    if not keep:
        return pd.DataFrame(columns=["created_at"])
    df2 = df[keep].copy()
    for c in [bpm_field, temp_field, hum_field, led_field]:
        if c in df2.columns:
            df2[c] = pd.to_numeric(df2[c], errors="coerce")
    return df2.dropna(subset=["created_at"])


def basic_stats(series: pd.Series) -> Dict[str, Optional[float]]:
    if series.dropna().empty:
        return {"mean": None, "min": None, "max": None}
    return {
        "mean": float(np.nanmean(series)),
        "min": float(np.nanmin(series)),
        "max": float(np.nanmax(series)),
    }


def count_led_activations(led_series: pd.Series) -> int:
    s = led_series.fillna(0).astype(int)
    prev = s.shift(1).fillna(0).astype(int)
    rises = ((prev != 1) & (s == 1)).sum()
    return int(rises)


def infer_sleep_stages_from_bpm(df: pd.DataFrame, bpm_field: str) -> Dict[str, Any]:
    if bpm_field not in df.columns or df[bpm_field].dropna().empty:
        return {"deep_h": 0.0, "light_h": 0.0, "rem_h": 0.0}

    d = df[["created_at", bpm_field]].dropna().sort_values("created_at").copy()
    if d.empty:
        return {"deep_h": 0.0, "light_h": 0.0, "rem_h": 0.0}

    # Mediana móvil como baseline (ventana ~15 muestras)
    d["baseline"] = d[bpm_field].rolling(window=15, min_periods=1).median()

    def stage_label(row):
        bpm = row[bpm_field]
        base = row["baseline"]
        if pd.isna(bpm) or pd.isna(base):
            return "light"
        if bpm <= base - 10:
            return "deep"
        elif bpm >= base + 5:
            return "rem"
        else:
            return "light"

    d["stage"] = d.apply(stage_label, axis=1)
    d["dt_sec"] = d["created_at"].shift(-1) - d["created_at"]
    d.loc[d.index[-1], "dt_sec"] = pd.Timedelta(seconds=30)  # asume 30s si no hay siguiente
    d["dt_sec"] = d["dt_sec"].dt.total_seconds().clip(lower=0)

    deep_h = d.loc[d["stage"] == "deep", "dt_sec"].sum() / 3600.0
    light_h = d.loc[d["stage"] == "light", "dt_sec"].sum() / 3600.0
    rem_h = d.loc[d["stage"] == "rem", "dt_sec"].sum() / 3600.0

    return {"deep_h": round(deep_h, 2), "light_h": round(light_h, 2), "rem_h": round(rem_h, 2)}


def sleep_quality(temp_stats: Dict[str, Optional[float]],
                  hum_stats: Dict[str, Optional[float]],
                  bpm_stats: Dict[str, Optional[float]],
                  led_count: int) -> Dict[str, Any]:
    # Temperatura ideal ~19°C
    t_mean = temp_stats.get("mean")
    if t_mean is None:
        score_temp = 50.0
    else:
        dist = min(abs(t_mean - 19.0), 6.0)
        score_temp = 100.0 * (1.0 - dist / 6.0)

    # Humedad ideal 40–60 %RH
    h_mean = hum_stats.get("mean")
    if h_mean is None:
        score_hum = 50.0
    else:
        if 40 <= h_mean <= 60:
            score_hum = 100.0
        else:
            dist = min(abs(h_mean - 50), 30.0)
            score_hum = max(0.0, 100.0 * (1.0 - dist / 30.0))

    # Variabilidad BPM (span)
    b_min, b_max = bpm_stats.get("min"), bpm_stats.get("max")
    if b_min is None or b_max is None:
        score_bpm = 50.0
    else:
        span = min(max(0.0, b_max - b_min), 40.0)
        score_bpm = 100.0 * (1.0 - span / 40.0)

    penalty = min(led_count * 5.0, 40.0)
    score = max(0.0, min(100.0, 0.35 * score_temp + 0.25 * score_hum + 0.40 * score_bpm - penalty))

    if score >= 85:
        label = "tu sueño fue casi perfecto"
    elif score >= 70:
        label = "dormiste bien"
    elif score >= 50:
        label = "dormiste regular"
    else:
        label = "dormiste muy mal"

    return {
        "score": round(score, 1),
        "label": label,
        "components": {
            "temp": round(score_temp, 1),
            "hum": round(score_hum, 1),
            "bpm": round(score_bpm, 1),
            "penalty": round(penalty, 1)
        }
    }


# =============================== Servicio CherryPy ==============================

class ReportsGenerator:
    exposed = True  # para MethodDispatcher

    def __init__(self, catalog_url: str, reports_url: str, thingspeak_url: str,
                 service_id: str = "ReportsGenerator", rest_endpoint: str = "http://reports_generator:8093"):
        self.catalog_url = catalog_url
        self.reports_url = reports_url
        self.ts_base = thingspeak_url
        self.service_id = service_id
        self.rest_endpoint = rest_endpoint

        # Nombres de campos TS (pueden venir por env, defaults razonables)
        self.f_bpm = os.environ.get("TS_BPM_FIELD", "field3")
        self.f_temp = os.environ.get("TS_TEMP_FIELD", "field1")
        self.f_hum  = os.environ.get("TS_HUM_FIELD",  "field2")
        self.f_led  = os.environ.get("TS_LED_FIELD",  "field7")

        # Padding para la query a TS (se recorta después)
        self.pad_min = int(os.environ.get("TS_PAD_MIN", "5"))

    @cherrypy.tools.json_out()
    def GET(self, user_id: str = "User1", date: Optional[str] = None):
        # Heartbeat al catálogo (best-effort)
        update_catalog_last_updated(self.catalog_url, self.service_id, self.rest_endpoint)

        # Fecha de referencia (hoy en Roma o la indicada)
        ref_date = now_rome().date() if not date else dateparser.parse(date).date()
        today_dt = datetime(ref_date.year, ref_date.month, ref_date.day, tzinfo=tz.gettz("Europe/Rome"))

        # 1) Traer usuario del catálogo
        user_obj = get_user_from_catalog(self.catalog_url, user_id)

        # 2) Horas de dormir/despertar -> ventana local
        t = extract_times(user_obj)
        start_dt, end_dt = window_for_date(t["timesleep"], t["timeawake"], today_dt)

        # 3) Credenciales TS desde catálogo (con override por env si existe)
        ts_info = extract_thingspeak(user_obj)
        channel_id = ts_info.get("channel") or os.environ.get("THINGSPEAK_CHANNEL_ID", "")
        keys: List[str] = ts_info.get("keys") or []
        env_key = os.environ.get("THINGSPEAK_READ_KEY", "").strip()
        if env_key:
            keys = [env_key] + [k for k in keys if k != env_key]

        # 4) Fetch de ThingSpeak con padding y estrategia robusta
        start_q = start_dt - timedelta(minutes=self.pad_min)
        end_q   = end_dt + timedelta(minutes=self.pad_min)

        try:
            df, dbg = fetch_ts_robusto(self.ts_base, channel_id, keys, start_q, end_q)
        except requests.RequestException as e:
            raise cherrypy.HTTPError(502, f"ThingSpeak error: {e}")

        # 5) Selección de campos y recorte final por ventana exacta
        if df.empty:
            return {
                "status": 200,
                "user_id": user_id,
                "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
                "message": "No data in the specified window",
                "metrics": {},
                "debug": dbg  # dejar mientras validas; quítalo si no quieres exponer
            }

        df = pick_fields(df, self.f_bpm, self.f_temp, self.f_hum, self.f_led)
        df = df[(df["created_at"] >= start_dt) & (df["created_at"] < end_dt)].copy()

        if df.empty:
            return {
                "status": 200,
                "user_id": user_id,
                "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
                "message": "No data after window clipping",
                "metrics": {},
                "debug": dbg
            }

        # 6) Métricas
        bpm_stats = basic_stats(df[self.f_bpm]) if self.f_bpm in df.columns else {"mean": None, "min": None, "max": None}
        temp_stats = basic_stats(df[self.f_temp]) if self.f_temp in df.columns else {"mean": None, "min": None, "max": None}
        hum_stats = basic_stats(df[self.f_hum]) if self.f_hum in df.columns else {"mean": None, "min": None, "max": None}
        led_count = count_led_activations(df[self.f_led]) if self.f_led in df.columns else 0

        stages = infer_sleep_stages_from_bpm(df, self.f_bpm)
        quality = sleep_quality(temp_stats, hum_stats, bpm_stats, led_count)

        return {
            "status": 200,
            "user_id": user_id,
            "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
            "stats": {
                "bpm": bpm_stats,
                "temperature": temp_stats,
                "humidity": hum_stats,
                "led_activations": led_count
            },
            "stages_hours": stages,
            "sleep_quality": quality
        }


# =================================== Arranque ===================================

if __name__ == "__main__":
    settings_file_path = os.path.join(os.path.dirname(__file__), 'settings.json')
    try:
        with open(settings_file_path, 'r') as f:
            settings = json.load(f)

        catalog_url = settings.get("catalogURL")
        reports_generator_url = settings.get("reportsURL")
        thingspeak_url = settings.get("thingspeakURL")

        service_info = settings.get("serviceInfo", {})
        service_id = service_info.get("serviceID", "ReportsGenerator")
        rest_endpoint = service_info.get("REST_endpoint", "http://reports_generator:8093")
        MQTT_sub_topics = service_info.get("MQTT_sub", [])
        MQTT_pub_topics = service_info.get("MQTT_pub", [])
    except Exception as e:
        print(f"Error reading settings: {e}")
        raise SystemExit(1)

    web_service = ReportsGenerator(
        catalog_url=catalog_url,
        reports_url=reports_generator_url,
        thingspeak_url=thingspeak_url,
        service_id=service_id,
        rest_endpoint=rest_endpoint
    )

    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True
        }
    }

    cherrypy.tree.mount(web_service, '/', conf)
    cherrypy.config.update({
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8093,
        'engine.autoreload.on': False
    })
    cherrypy.engine.start()
    cherrypy.engine.block()
