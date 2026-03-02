
import json
import time
from datetime import datetime
import threading
import logging
import cherrypy
import os

# -------- Logging --------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("catalog")

# -------- Config --------
CATALOG_PATH = os.getenv("CATALOG_PATH", "catalog.json")
WRITE_TOKEN = os.getenv("CATALOG_WRITE_TOKEN")  # optional
READ_ONLY = os.getenv("CATALOG_READ_ONLY", "false").lower() == "true"

def now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def ensure_parent(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

class CatalogService:
    def __init__(self, json_path: str):
        self.json_path = json_path
        self._lock = threading.RLock()
        self._catalog = None  # lazy load
        self._last_load = 0.0
        self._cache_ttl = float(os.getenv("CATALOG_CACHE_TTL", "2.0"))  # seconds

    # ------------- Storage helpers -------------
    def _load_from_disk(self) -> dict:
        if not os.path.exists(self.json_path):
            raise FileNotFoundError(f"No existe {self.json_path}")
        with open(self.json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_to_disk(self, payload: dict):
        ensure_parent(self.json_path)
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _get_catalog(self) -> dict:
        with self._lock:
            expired = (time.time() - self._last_load) > self._cache_ttl
            if self._catalog is None or expired:
                self._catalog = self._load_from_disk()
                self._last_load = time.time()
            return self._catalog

    def _replace_catalog(self, payload: dict):
        with self._lock:
            payload.setdefault("lastUpdate", now_str())
            self._validate_minimal(payload)
            self._save_to_disk(payload)
            self._catalog = payload
            self._last_load = time.time()

    # ------------- Minimal schema validation -------------
    def _validate_minimal(self, data: dict):
        required = ["catalog_url", "projectOwners", "project_name",
                    "broker", "servicesList", "devicesList", "roomsList", "usersList"]
        for k in required:
            if k not in data:
                raise cherrypy.HTTPError(400, f"Falta campo obligatorio: {k}")
        # types sanity
        if not isinstance(data["servicesList"], list): raise cherrypy.HTTPError(400, "servicesList debe ser lista")
        if not isinstance(data["devicesList"], list):  raise cherrypy.HTTPError(400, "devicesList debe ser lista")
        if not isinstance(data["roomsList"], list):    raise cherrypy.HTTPError(400, "roomsList debe ser lista")
        if not isinstance(data["usersList"], list):    raise cherrypy.HTTPError(400, "usersList debe ser lista")

    # ------------- Utilities -------------
    @staticmethod
    def _json_response(obj, status=200):
        cherrypy.response.headers["Content-Type"] = "application/json; charset=utf-8"
        cherrypy.response.status = status
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def _require_token():
        if WRITE_TOKEN:
            token = cherrypy.request.headers.get("X-Write-Token")
            if token != WRITE_TOKEN:
                raise cherrypy.HTTPError(401, "Token inválido")

    # ------------- HTTP endpoints -------------
    @cherrypy.expose
    def health(self):
        return self._json_response({"status": "ok", "time": datetime.utcnow().isoformat() + "Z"})

    @cherrypy.expose
    def index(self):
        return self._json_response({"see": "/catalog"})

    @cherrypy.expose
    def catalog(self, **kwargs):
        method = cherrypy.request.method.upper()
        if method == "GET":
            try:
                return self._json_response(self._get_catalog())
            except Exception as e:
                logger.exception("Error leyendo catálogo")
                raise cherrypy.HTTPError(500, str(e))
        elif method in ("PUT", "POST"):
            if READ_ONLY:
                raise cherrypy.HTTPError(403, "Read-only")
            self._require_token()
            try:
                raw = cherrypy.request.body.read().decode("utf-8")
                payload = json.loads(raw) if raw else {}
                self._replace_catalog(payload)
                return self._json_response({"status": "updated", "lastUpdate": self._catalog["lastUpdate"]})
            except cherrypy.HTTPError:
                raise
            except Exception as e:
                logger.exception("Error actualizando catálogo")
                raise cherrypy.HTTPError(400, f"Payload inválido: {e}")
        else:
            raise cherrypy.HTTPError(405)

    # ----- Collections: services, devices, rooms, users -----
    @cherrypy.expose
    def services(self, serviceID=None, **kwargs):
        return self._resource_handler("servicesList", "serviceID", serviceID)

    @cherrypy.expose
    def devices(self, deviceID=None, **kwargs):
        return self._resource_handler("devicesList", "deviceID", deviceID)

    @cherrypy.expose
    def rooms(self, roomID=None, **kwargs):
        return self._resource_handler("roomsList", "roomID", roomID)

    @cherrypy.expose
    def users(self, userID=None, **kwargs):
        return self._resource_handler("usersList", "userID", userID)

    # Generic CRUD handler mirroring common pattern in your reference
    def _resource_handler(self, list_key: str, id_key: str, resource_id: str | None):
        method = cherrypy.request.method.upper()
        data = self._get_catalog()

        def find_index(seq, key, value):
            for i, item in enumerate(seq):
                if item.get(key) == value:
                    return i
            return -1

        try:
            collection = data[list_key]
        except KeyError:
            raise cherrypy.HTTPError(500, f"Catálogo corrupto: falta {list_key}")

        if method == "GET":
            if resource_id is None:
                return self._json_response(collection)
            idx = find_index(collection, id_key, resource_id)
            if idx < 0:
                raise cherrypy.HTTPError(404, f"{id_key} '{resource_id}' no encontrado")
            return self._json_response(collection[idx])

        if READ_ONLY:
            raise cherrypy.HTTPError(403, "Read-only")
        self._require_token()

        raw = cherrypy.request.body.read().decode("utf-8") if cherrypy.request.body else ""
        payload = json.loads(raw) if raw else {}

        if method == "POST":
            # create
            if id_key not in payload:
                raise cherrypy.HTTPError(400, f"Falta {id_key} en payload")
            if find_index(collection, id_key, payload[id_key]) >= 0:
                raise cherrypy.HTTPError(409, f"{id_key} ya existe")
            payload.setdefault("timestamp", now_str())
            collection.append(payload)
            self._replace_catalog(data)
            return self._json_response(payload, status=201)

        elif method in ("PUT", "PATCH"):
            # update/replace
            if resource_id is None:
                raise cherrypy.HTTPError(400, f"Especifica {id_key} en la URL")
            idx = find_index(collection, id_key, resource_id)
            if idx < 0:
                raise cherrypy.HTTPError(404, f"{id_key} '{resource_id}' no encontrado")
            # merge for PATCH-like
            if method == "PATCH":
                collection[idx].update(payload)
            else:
                payload.setdefault(id_key, resource_id)
                collection[idx] = payload
            collection[idx]["timestamp"] = now_str()
            self._replace_catalog(data)
            return self._json_response(collection[idx])

        elif method == "DELETE":
            if resource_id is None:
                raise cherrypy.HTTPError(400, f"Especifica {id_key} en la URL")
            idx = find_index(collection, id_key, resource_id)
            if idx < 0:
                raise cherrypy.HTTPError(404, f"{id_key} '{resource_id}' no encontrado")
            removed = collection.pop(idx)
            self._replace_catalog(data)
            return self._json_response({"deleted": removed.get(id_key)})

        else:
            raise cherrypy.HTTPError(405)

# --------- Server bootstrap ---------
def run():
    svc = CatalogService(CATALOG_PATH)
    conf = {
        "/": {
            "tools.response_headers.on": True,
            "tools.response_headers.headers": [
                ("Access-Control-Allow-Origin", "*"),
                ("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS"),
                ("Access-Control-Allow-Headers", "Content-Type, X-Write-Token"),
                ("Content-Type", "application/json; charset=utf-8"),
            ],
        }
    }

    class CORS(object):
        @cherrypy.tools.register("before_handler")
        def cors():
            if cherrypy.request.method == "OPTIONS":
                cherrypy.response.headers["Access-Control-Allow-Origin"] = "*"
                cherrypy.response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
                cherrypy.response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Write-Token"
                cherrypy.response.status = 204
                return True

    cherrypy.tools.cors = CORS.cors
    cherrypy.config.update({"server.socket_host": "0.0.0.0",
                            "server.socket_port": int(os.getenv("PORT", "9080")),
                            "log.screen": True})
    cherrypy.quickstart(svc, config=conf)

if __name__ == "__main__":
    run()
