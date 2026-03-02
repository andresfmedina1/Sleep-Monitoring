import json

def parse_senml(payload: str):
    pack = json.loads(payload)
    out = []
    for rec in pack:
        bn = rec.get("bn", "").rstrip("/")  # <- aquí
        bt = rec.get("bt", 0)
        for e in rec.get("e", []):
            n  = e.get("n")
            u  = e.get("u")
            v  = e.get("v", None)
            vb = e.get("vb", None)
            vs = e.get("vs", None)
            t  = e.get("t", 0)
            ts = bt + t
            val = v if v is not None else (vb if vb is not None else vs)
            out.append(((f"{bn}/{n}") if bn else n, u, val, ts))
    return out


def build_senml(device_id:str, entries:list, base_time:int=0):
    return json.dumps([{"bn":f"{device_id}/","bt":base_time,"e":entries}])