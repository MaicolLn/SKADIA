#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza holdings en json_scada.realtimeData con el estado real de coils,
leyendo del PLC mediante la función del usuario: leer_plc(nodo, linea) -> bool.

- Carga TAG_TO_ACTION y WATCH_TAGS desde tags_ctrl.json
- Para cada tag de WATCH_TAGS, resuelve (nodo, linea) y lee el estado con leer_plc
- Escribe en realtimeData: lastValue/value/valueString y timestamps
- No modifica configuraciones del tag (addresses, links, etc.)
"""

from pathlib import Path
from datetime import datetime, timezone
import json
from typing import Tuple

from pymongo import MongoClient
from pymongo.errors import PyMongoError

# 👉 IMPORTA TU FUNCIÓN AQUÍ (ajusta el nombre del módulo si es distinto)
# Se asume: leer_plc(nodo: str, linea: str) -> bool
from tool_f.modbus_tools import leer_medidor, escribir_plc, leer_plc

# --------------------------
# Config Mongo / Paths
# --------------------------
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME   = "json_scada"
COL_RT    = "realtimeData"

BASE_DIR = Path(__file__).resolve().parent
MAPPING_PATH = BASE_DIR / "tags_ctrl.json"

# --------------------------
# Utilidades
# --------------------------
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

    # Normaliza [nodo, linea] -> (nodo, linea)
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

def bool_to_strings(v: bool) -> Tuple[int, str, str]:
    """Devuelve (value, valueString, valueJson) coherentes con booleano."""
    return (1 if v else 0, "true" if v else "false", "true" if v else "false")

def now_utc():
    return datetime.now(timezone.utc)

# --------------------------
# Sincronización
# --------------------------
def sync_once(client: MongoClient) -> None:
    """Ejecuta una pasada de sincronización (lee coils y actualiza holdings)."""
    TAG_TO_ACTION, WATCH_TAGS = load_mapping(MAPPING_PATH)

    db  = client[DB_NAME]
    col = db[COL_RT]

    updated = 0
    skipped = 0
    errors  = 0

    for tag in WATCH_TAGS:
        action = TAG_TO_ACTION.get(tag)
        if not action:
            print(f"[SKIP] Sin mapeo para {tag}")
            skipped += 1
            continue

        nodo, linea = action
        try:
            coil_state = bool(leer_plc(nodo, linea, timeout=1.5))  # <- tu función
        except Exception as e:
            print(f"[ERR] leer_plc('{nodo}', '{linea}') falló: {e}")
            errors += 1
            continue

        v, vs, vj = bool_to_strings(coil_state)
        ts = now_utc()

        # Construye el $set con campos típicos de realtimeData
        set_fields = {
            "lastValue": bool(coil_state),
            "value": v,
            "valueString": vs,
            "valueJson": vj,
            "timeTag": ts,
            # Opcional: deja huella en sourceDataUpdate sin romper formatos
            "sourceDataUpdate.valueAtSource": v,
            "sourceDataUpdate.valueStringAtSource": vs,
            "sourceDataUpdate.valueJsonAtSource": vj,
            "sourceDataUpdate.timeTag": ts,
        }

        try:
            res = col.update_one(
                {"tag": tag},
                {"$set": set_fields},
                upsert=False  # no creamos tags nuevos; solo sincronizamos existentes
            )
            if res.matched_count == 0:
                print(f"[WARN] Tag '{tag}' no existe en {DB_NAME}.{COL_RT}. (no actualizado)")
                skipped += 1
            else:
                print(f"[OK]  {tag}: {coil_state}  (lastValue/valueString actualizados)")
                updated += 1
        except PyMongoError as e:
            print(f"[ERR] Mongo update {tag} falló: {e}")
            errors += 1

    print(f"\nResumen: updated={updated}  skipped={skipped}  errors={errors}")

# --------------------------
# Entrypoint
# --------------------------
def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        # Una pasada (modo prueba). Si quieres loop, envuelve en while con sleep.
        sync_once(client)
    finally:
        client.close()

if __name__ == "__main__":
    main()
