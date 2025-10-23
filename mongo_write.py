#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from tool_f.modbus_tools import leer_medidor, escribir_plc, leer_plc

"""
Watcher de holdings 4x en json_scada.realtimeData.
- Carga TAG_TO_ACTION y WATCH_TAGS desde tags_ctrl.json
- Usa Change Streams para detectar cambios en tiempo real
- Detecta flancos (subida/bajada) y llama un hook que imprime la acción
- NO envía comandos al PLC (solo muestra lo que harías)
"""

from pathlib import Path
from datetime import datetime
import json
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

# =========================
# CONFIG MongoDB
# =========================
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME   = "json_scada"
COL_RT    = "realtimeData"

# Ruta del JSON de mapeo (mismo directorio del script)
BASE_DIR = Path(__file__).resolve().parent
MAPPING_PATH = BASE_DIR / "tags_ctrl.json"

# =========================
# Utilidades
# =========================
def load_mapping(json_path: Path):
    """Carga TAG_TO_ACTION y WATCH_TAGS desde un JSON y valida lo básico."""
    if not json_path.exists():
        raise FileNotFoundError(f"No encuentro el archivo de mapeo: {json_path}")

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"{json_path.name} no es JSON válido: {e}")

    tag_to_action_raw = data.get("TAG_TO_ACTION", {})
    watch_tags = list(data.get("WATCH_TAGS", []))

    # Normaliza las parejas [nodo, linea] a tuplas (nodo, linea)
    tag_to_action = {}
    for tag, pair in tag_to_action_raw.items():
        if (not isinstance(pair, (list, tuple))) or len(pair) != 2:
            raise ValueError(f"TAG_TO_ACTION['{tag}'] debe ser [\"Nodo_X\", \"linea_xxx\"], recibido: {pair}")
        nodo, linea = pair
        tag_to_action[tag] = (str(nodo), str(linea))

    # Aviso si hay WATCH_TAGS sin acción mapeada
    missing = [t for t in watch_tags if t not in tag_to_action]
    if missing:
        print(f"[WARN] WATCH_TAGS sin acción definida en TAG_TO_ACTION: {missing}")

    return tag_to_action, watch_tags

def as_boolish(v):
    """Normaliza 0/1, 'true'/'false', True/False a booleano; si no, devuelve tal cual."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("true", "1", "on"):
            return True
        if t in ("false", "0", "off"):
            return False
    return v  # deja tal cual (por si viniera entero 16-bit que no sea 0/1)

def extract_value(doc):
    """Extrae el valor útil desde el documento de realtimeData."""
    for k in ("lastValue", "value", "valueString", "valueJson"):
        if doc.get(k) is not None:
            return doc[k]
    return None

# =========================
# Hook (NO envía; solo imprime)
# =========================
def on_holding_change(tag_holding: str, nuevo_valor_bool: bool, addr: str, TAG_TO_ACTION: dict):
    action = TAG_TO_ACTION.get(tag_holding)
    if not action:
        print(f"[SKIP] Sin mapeo para {tag_holding}")
        return

    nodo, linea = action

    try:
        # 👉 AQUÍ haces el mando real
        ok = escribir_plc(nodo, linea, bool(nuevo_valor_bool))
        print(f"[CMD] {tag_holding}({addr}) -> {nodo}.{linea} = {nuevo_valor_bool} | ok={ok}")
    except Exception as e:
        print(f"[ERR] escribir_plc({nodo}, {linea}, {nuevo_valor_bool}) falló: {e}")


# =========================
# Watcher principal
# =========================
def watch_holdings():
    TAG_TO_ACTION, WATCH_TAGS = load_mapping(MAPPING_PATH)
    print(f"[INIT] Cargado {len(TAG_TO_ACTION)} mapeos. Vigilando tags: {WATCH_TAGS}")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = client[DB_NAME]

    # Pipeline: solo cambios y solo para los tags que te interesan
    pipeline = [
        {"$match": {"operationType": {"$in": ["update", "replace"]}}},
        {"$match": {"fullDocument.tag": {"$in": WATCH_TAGS}}},
    ]

    # Memoria de último valor por tag para detectar flancos y suprimir duplicados
    last_seen = {}      # tag -> valor normalizado
    debounce_ms = 100   # anti-rebote simple entre emisiones por tag
    last_emit_ts = {}   # tag -> tiempo de última emisión (seg)

    while True:
        try:
            print("[WATCH] Suscribiendo Change Stream… (Ctrl+C para salir)")
            with db[COL_RT].watch(pipeline=pipeline, full_document="updateLookup") as stream:
                for ev in stream:
                    fd   = ev.get("fullDocument") or {}
                    tag  = fd.get("tag")
                    addr = fd.get("protocolSourceObjectAddress")
                    raw  = extract_value(fd)
                    val  = as_boolish(raw)

                    # suprime “no cambios” de valor
                    prev = last_seen.get(tag)
                    if prev == val:
                        continue

                    # anti-rebote por tiempo entre emisiones
                    now_s = time.time()
                    last_ts = last_emit_ts.get(tag, 0)
                    if (now_s - last_ts) * 1000.0 < debounce_ms:
                        last_seen[tag] = val  # igual actualiza el baseline
                        continue
                    last_emit_ts[tag] = now_s

                    # log básico
                    print(f"[{datetime.now().isoformat(timespec='seconds')}] "
                          f"TAG={tag}  ADDR={addr}  NEW_VALUE={val}  (raw={raw})")

                    # actualiza baseline
                    last_seen[tag] = val

                    # dispara flancos
                    if prev is None:
                        # primera muestra: no dispares acción
                        continue
                    if prev is False and val is True:
                        # flanco de subida → ON
                        on_holding_change(tag, True, addr, TAG_TO_ACTION)
                    elif prev is True and val is False:
                        # flanco de bajada → OFF
                        on_holding_change(tag, False, addr, TAG_TO_ACTION)

        except PyMongoError as e:
            # si el primario cambia o hay un corte, reintenta con backoff
            print(f"[ERROR] Change Stream: {e}")
            print("[RETRY] Reintentando en 2 s…")
            time.sleep(2.0)
        except KeyboardInterrupt:
            print("\n[SIGNAL] Interrumpido por el usuario. Saliendo…")
            break

# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    watch_holdings()
