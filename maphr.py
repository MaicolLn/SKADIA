# build_node_maps.py  (run once; or embed in your app init)
import re, json
from pathlib import Path
from pymongo import MongoClient

# --- Mongo ---
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME   = "json_scada"
COL_RT    = "realtimeData"

# --- Files (next to script) ---
BASE_DIR = Path(__file__).resolve().parent
HR_MAP_PATH          = BASE_DIR / "hr_map.json"            # tag -> {table, addr_1b, idx0, dtype}
TAG_TO_ACTION_PATH   = BASE_DIR / "tags_ctrl.json"         # your {"TAG_TO_ACTION": {...}, ...}
NODE_ELEM_ENTRIES_PATH = BASE_DIR / "node_elem_entries.json"  # built: node -> elem -> [entries...]
NODE_ELEM_SLOT_PATH  = BASE_DIR / "node_elem_slot.json"    # built: node -> {elem -> slot}

ADDR_RE = re.compile(r"^(?P<table>[014]x)(?P<addr>\d{1,5})(?::(?P<dtype>\w+))?$")
TAG_CMD_RE = re.compile(r"^NODO_(?P<node>\d+)_CMD(?P<cmd>\d+)$", re.IGNORECASE)

def fetch_hr_map():
    cli = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = cli[DB_NAME]
    cur = db[COL_RT].find(
        {"protocolSourceObjectAddress": {"$regex": r"^[014]x\d+"}},
        {"tag":1, "protocolSourceObjectAddress":1, "_id":0}
    )
    hr_map = {}
    for doc in cur:
        tag = doc.get("tag")
        addr = (doc.get("protocolSourceObjectAddress") or "").strip()
        m = ADDR_RE.match(addr)
        if not tag or not m: 
            continue
        table = m.group("table")      # "4x", "1x", "0x"
        a     = int(m.group("addr"))  # 1-based shown in string
        dtype = m.group("dtype") or None
        hr_map[tag] = {"table": table, "addr_1b": a, "idx0": max(0, a-1), "dtype": dtype}
    HR_MAP_PATH.write_text(json.dumps(hr_map, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[MAP] hr_map: {len(hr_map)} entries -> {HR_MAP_PATH}")
    return hr_map

def load_tag_to_action():
    data = json.loads(TAG_TO_ACTION_PATH.read_text(encoding="utf-8"))
    t2a = data.get("TAG_TO_ACTION", {})
    # normalize to dict[tag] -> (node_label, element_name)
    norm = {}
    for tag, pair in t2a.items():
        if isinstance(pair, (list, tuple)) and len(pair)==2:
            norm[str(tag)] = (str(pair[0]), str(pair[1]))
    return norm

def build_node_maps():
    hr_map = fetch_hr_map()
    tag_to_action = load_tag_to_action()

    # node -> element -> list of entries {cmd_idx, tag, addr_1b, idx0}
    node_elem_entries = {}

    for tag, (node_label, elem_name) in tag_to_action.items():
        # Only consider tags we can resolve to 4x HR
        info = hr_map.get(tag)
        if not info or info.get("table") != "4x":
            continue

        # Parse CMD number to sort within each element group
        m = TAG_CMD_RE.match(tag)
        cmd_idx = int(m.group("cmd")) if m else 0

        entry = {
            "cmd_idx": cmd_idx,
            "tag": tag,
            "addr_1b": int(info["addr_1b"]),
            "idx0": int(info["idx0"])
        }

        node_elem_entries.setdefault(node_label, {}).setdefault(elem_name, []).append(entry)

    # Sort each element’s entries by cmd index (or addr_1b)
    for node, emap in node_elem_entries.items():
        for elem, lst in emap.items():
            lst.sort(key=lambda e: (e["cmd_idx"], e["addr_1b"]))

    # Build element → slot map per node (your numbering)
    node_elem_slot = {}
    for node, emap in node_elem_entries.items():
        # Assign stable, human-friendly order:
        #  - keep "Generación 1/2/3" in numeric order
        #  - then others alphabetically
        def elem_key(name: str):
            m = re.search(r"(\d+)$", name.strip())
            return (0, int(m.group(1))) if m else (1, name.lower())
        ordered = sorted(emap.keys(), key=elem_key)
        node_elem_slot[node] = {elem: i for i, elem in enumerate(ordered)}

    # Save both
    NODE_ELEM_ENTRIES_PATH.write_text(json.dumps(node_elem_entries, indent=2, ensure_ascii=False), encoding="utf-8")
    NODE_ELEM_SLOT_PATH.write_text(json.dumps(node_elem_slot, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[MAP] node_elem_entries -> {NODE_ELEM_ENTRIES_PATH}")
    print(f"[MAP] node_elem_slot    -> {NODE_ELEM_SLOT_PATH}")

if __name__ == "__main__":
    build_node_maps()
