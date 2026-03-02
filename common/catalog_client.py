import os, time, requests

class CatalogClient:
    """Cliente ligero con cache para el catálogo JSON."""
    def __init__(self, url: str | None = None, ttl: float = 5.0, timeout: float = 5.0):
        self.url = url or os.getenv("CATALOG_URL", "http://catalog:9080/catalog")
        self.ttl = ttl
        self.timeout = timeout
        self._cache = None
        self._last = 0.0

    def _fetch(self):
        r = requests.get(self.url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def get(self, force: bool = False) -> dict:
        now = time.time()
        if force or self._cache is None or now - self._last > self.ttl:
            self._cache = self._fetch()
            self._last = now
        return self._cache

    # -------- helpers de negocio --------
    def broker(self) -> tuple[str, int]:
        c = self.get()
        b = c.get("broker", {})
        return b.get("IP", "test.mosquitto.org"), int(b.get("port", 1883))

    def service(self, service_id: str) -> dict | None:
        for s in self.get().get("servicesList", []):
            if s.get("serviceID") == service_id:
                return s
        return None

    def users_map_api_keys(self) -> dict[tuple[str, str], str]:
        """
        Devuelve {(userID, roomID): write_api_key}
        Toma el primer apikey de usersList[].thingspeak_info.apikeys
        """
        out: dict[tuple[str, str], str] = {}
        for u in self.get().get("usersList", []):
            uid = u.get("userID")
            room = u.get("roomID", "Room1")
            ts = (u.get("thingspeak_info") or {})
            keys = ts.get("apikeys") or []
            if uid and room and keys:
                out[(uid, room)] = keys[0]
        return out
