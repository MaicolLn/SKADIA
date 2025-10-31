#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from tool_f.modbus_tools import leer_medidor, escribir_plc, leer_plc
import threading, time, random
from plc4_ctrl import stop_plc4x, start_plc4x
import revive 
PLC4X_WINDOW_LOCK      = threading.Lock()  # evita ventanas solapadas
PRE_WRITE_PAUSE_S      = 0.6               # 300–800 ms recomendado
POST_WRITE_VERIFY_S    = 0.12              # espera breve antes de leer/verificar (si aplica)

"""
Watcher de holdings 4x en json_scada.realtimeData.
- Carga TAG_TO_ACTION y WATCH_TAGS desde tags_ctrl.json
- Usa Change Streams para detectar cambios en tiempo real
- Detecta flancos (subida/bajada) y ejecuta on_holding_change()
"""

from pathlib import Path
from datetime import datetime
import json
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ===== Arriba de tu archivo =====
import struct
from typing import Tuple, Sequence, Optional
from pymodbus.client import ModbusTcpClient
from pymodbus.constants import Endian
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

MAX_RETRIES  = 8
TIMEOUT_S    = 3         # segundos por intento escribir_plc
BASE_BACKOFF = 0.25      # s (exponencial con jitter)
MAX_BACKOFF  = 2         # s
# Ruta del JSON de métricas (junto al script)
import os
from pathlib import Path

# Same pattern as MAPPING_PATH
BASE_DIR = Path(__file__).resolve().parent
MEASURES_PATH = BASE_DIR / "measures_time.json"

# --- Fallback HR target (by default, reuse your main Modbus host/port) ---
MB_HOST = "127.0.0.1"
MB_PORT = 1502
MB_UNIT = 1

MB_UNIT = 1  # ajusta al Unit-ID real del PLC/gateway

# --- Nodo (numérico) -> índice 0-based del Holding Register (4x00001 ≡ idx0=0) ---
NODE_TO_HR_IDX: dict[int, int] = {
    645: 20,   # 4x00021
    634: 37,
    646: 52,
    675: 84,
    611: 68,
    692: 100,
}

import re
import re
from typing import Optional  # ← add this

def _extract_node_int(nodo_str: str) -> Optional[int]:   # ← change here
    m = re.search(r"(\d+)", str(nodo_str))
    return int(m.group(1)) if m else None


def _write_hr_bool_once(idx0: int, desired: bool) -> bool:
    """
    Escribe 0/1 en HR idx0 (0-based) usando tus helpers.
    Usa write_hr_u16 con reg_1b = idx0 + 1.
    """
    try:
        reg_1b = int(idx0) + 1
        write_hr_u16(
            host=MB_HOST,
            port=MB_PORT,
            slave=MB_UNIT,
            reg_1b=reg_1b,
            value=(1 if desired else 0),
            timeout=MODBUS_TIMEOUT,
        )
        print(f"[DR] HR WRITE fallback: idx0={idx0} (4x{reg_1b:05d}) <- {1 if desired else 0}")
        return True
    except Exception as e:
        print(f"[DR] ERROR fallback HR idx0={idx0} (4x{idx0+1:05d}) <- {1 if desired else 0}: {e}")
        return False

def _fallback_set_opposite(nodo_str: str, desired_bool: bool) -> None:
    """
    Si fallan todos los intentos vía escribir_plc, fuerza el HR del 'nodo' al opuesto.
    """
    node = _extract_node_int(nodo_str)
    if node is None:
        print(f"[DR] (WARN) no pude extraer número de nodo desde '{nodo_str}'")
        return
    idx0 = NODE_TO_HR_IDX.get(node)
    if idx0 is None:
        print(f"[DR] (WARN) Nodo {node} sin HR mapeado; completa NODE_TO_HR_IDX.")
        return
    opposite = (not bool(desired_bool))
    _write_hr_bool_once(idx0, opposite)



def call_with_timeout(func, *args, timeout=TIMEOUT_S, **kwargs):
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(func, *args, **kwargs)
        return fut.result(timeout=timeout)

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
# === UTILIDADES HR SEGURAS (U16 / FLOAT32, con endianness controlado) ===
# =========================

# Ajusta estos si tu dispositivo usa otro orden:
HR_BYTEORDER = "big"   # "big" | "little"  -> bytes dentro de cada registro de 16 bits
HR_WORDORDER = "big"   # "big" | "little"  -> orden de las palabras de 16 bits (HiWord primero = "big")

MODBUS_HOST = "192.168.1.200"
MODBUS_PORT = 502
MODBUS_TIMEOUT = 3.0

def _endianness() -> Tuple[str, str]:
    return HR_BYTEORDER, HR_WORDORDER

def float32_to_registers(value: float) -> Tuple[int, int]:
    """
    Convierte un float32 en dos registros de 16 bits según HR_BYTEORDER/HR_WORDORDER.
    Por defecto BIG/BIG ⇒ 645.0 -> [0x4421, 0x4000] = [17441, 16384]
    """
    border, worder = _endianness()
    # Empaquetar a 4 bytes en big-endian (IEEE754) y luego reordenar si hace falta
    be_bytes = struct.pack(">f", float(value))  # siempre genero en BE base
    hi, lo = be_bytes[:2], be_bytes[2:]

    if worder == "big":
        word_hi, word_lo = hi, lo
    else:
        word_hi, word_lo = lo, hi

    def b2u16(b: bytes, border: str) -> int:
        return int.from_bytes(b, byteorder=("big" if border == "big" else "little"), signed=False)

    reg_hi = b2u16(word_hi, border)
    reg_lo = b2u16(word_lo, border)
    return reg_hi, reg_lo

MEASURES_LOCK = threading.Lock()

def append_measure(nodo: str, dt_s: float, command: bool) -> None:
    rec = {
        "nodo": str(nodo),
        "dt_s": round(float(dt_s), 6),
        "command": bool(command),
    }

    with MEASURES_LOCK:
        try:
            # read current list (or start empty)
            if MEASURES_PATH.exists():
                try:
                    data = json.loads(MEASURES_PATH.read_text(encoding="utf-8"))
                    if not isinstance(data, list):
                        data = []
                except json.JSONDecodeError:
                    data = []
            else:
                data = []

            data.append(rec)

            # write back (simple write; use temp+replace if you want atomicity)
            MEASURES_PATH.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        except Exception as e:
            print(f"[WARN] No se pudo guardar measures_time: {e}")

def registers_to_float32(regs: Sequence[int]) -> float:
    """
    Convierte dos registros en float32 respetando HR_BYTEORDER/HR_WORDORDER.
    """
    assert len(regs) >= 2, "Se requieren 2 registros para float32"
    border, worder = _endianness()

    def u16_to_bytes(x: int, border: str) -> bytes:
        return int(x & 0xFFFF).to_bytes(2, byteorder=("big" if border == "big" else "little"), signed=False)

    b_hi = u16_to_bytes(regs[0], border)
    b_lo = u16_to_bytes(regs[1], border)
    be_bytes = (b_hi + b_lo) if worder == "big" else (b_lo + b_hi)
    return struct.unpack(">f", be_bytes)[0]

def write_hr_u16(*, host=MODBUS_HOST, port=MODBUS_PORT, slave: int, reg_1b: int, value: int, timeout: float = MODBUS_TIMEOUT) -> None:
    """
    Escribe un entero U16 en HR[reg_1b] (1-based).
    """
    addr0 = int(reg_1b) - 1
    cli = ModbusTcpClient(host, port=port, timeout=timeout, strict=False, retry_on_empty=True, retries=2)
    if not cli.connect():
        raise RuntimeError("No conecta Modbus")
    try:
        wr = cli.write_register(address=addr0, value=int(value) & 0xFFFF, slave=slave)
        if not wr or wr.isError():
            raise RuntimeError(f"WRITE U16 HR[{reg_1b}] fallo: {wr}")
    finally:
        cli.close()

def write_hr_float32(*, host=MODBUS_HOST, port=MODBUS_PORT, slave: int, reg_1b: int, value: float, timeout: float = MODBUS_TIMEOUT) -> None:
    """
    Escribe un float32 en HR[reg_1b..reg_1b+1] (1-based) con endianness controlado.
    """
    addr0 = int(reg_1b) - 1
    r0, r1 = float32_to_registers(value)
    cli = ModbusTcpClient(host, port=port, timeout=timeout, strict=False, retry_on_empty=True, retries=2)
    if not cli.connect():
        raise RuntimeError("No conecta Modbus")
    try:
        wr = cli.write_registers(address=addr0, values=[r0, r1], slave=slave)
        if not wr or wr.isError():
            raise RuntimeError(f"WRITE F32 HR[{reg_1b}]..+1 fallo: {wr}")
    finally:
        cli.close()

def read_hr_u16(*, host=MODBUS_HOST, port=MODBUS_PORT, slave: int, reg_1b: int, timeout: float = MODBUS_TIMEOUT) -> int:
    addr0 = int(reg_1b) - 1
    cli = ModbusTcpClient(host, port=port, timeout=timeout, strict=False)
    if not cli.connect():
        raise RuntimeError("No conecta Modbus")
    try:
        rr = cli.read_holding_registers(address=addr0, count=1, slave=slave)
        if not rr or rr.isError():
            raise RuntimeError(f"READ U16 HR[{reg_1b}] fallo: {rr}")
        return int(rr.registers[0] & 0xFFFF)
    finally:
        cli.close()

def read_hr_float32(*, host=MODBUS_HOST, port=MODBUS_PORT, slave: int, reg_1b: int, timeout: float = MODBUS_TIMEOUT) -> float:
    addr0 = int(reg_1b) - 1
    cli = ModbusTcpClient(host, port=port, timeout=timeout, strict=False)
    if not cli.connect():
        raise RuntimeError("No conecta Modbus")
    try:
        rr = cli.read_holding_registers(address=addr0, count=2, slave=slave)
        if not rr or rr.isError():
            raise RuntimeError(f"READ F32 HR[{reg_1b}]..+1 fallo: {rr}")
        return registers_to_float32(rr.registers[:2])
    finally:
        cli.close()

# =========================
# Utilidades watcher
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
# Handler de cambios
# =========================
def on_holding_change(tag_holding: str, nuevo_valor_bool: bool, addr: str, TAG_TO_ACTION: dict):
    action = TAG_TO_ACTION.get(tag_holding)
    if not action:
        print(f"[SKIP] Sin mapeo para {tag_holding}")
        return
    
    nodo, linea = action

    # serializa ventanas stop→write→start
    with PLC4X_WINDOW_LOCK:
        t0 = time.perf_counter()
        print("[MAINT] Deteniendo plc4xclient para ventana de mando…")
        stop_plc4x()  # maneja timeouts/retries internos

        try:
            # pequeña pausa para asegurar que el gateway liberó la sesión TCP
            time.sleep(PRE_WRITE_PAUSE_S)

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # medir el tiempo SOLO de la llamada a escribir_plc
                    
                    ok = call_with_timeout(
                        escribir_plc, nodo, linea, bool(nuevo_valor_bool),
                        timeout=TIMEOUT_S
                    )
                    dt = time.perf_counter() - t0  # tiempo empleado en la llamada

                    # “empujoncito” opcional al PLC4X haciendo lectura simple
                    revive.ping_medidores_hr3036(
                        ruta_json="mapaIEEE13.json",
                        host=MODBUS_HOST,
                        port=MODBUS_PORT,
                        timeout=2.0,
                        prefer_param="VLN_AVG",
                    )
                    time.sleep(POST_WRITE_VERIFY_S)

                    if ok:
                        # guardar métrica (nodo + delta t)
                        append_measure(nodo=nodo, dt_s=dt, command=True)
                        print(f"[CMD] {tag_holding}({addr}) -> {nodo}.{linea} = {nuevo_valor_bool} | ok={ok} | attempt={attempt} | dt_s={dt:.6f}")
                        return
                    else:
                        raise RuntimeError("escribir_plc devolvió False/None")
                except (FuturesTimeout, Exception) as e:
                    print(f"[WARN] escribir_plc fallo intento {attempt}/{MAX_RETRIES} para {nodo}.{linea}: {e}")
                    if attempt < MAX_RETRIES:
                        backoff = min(BASE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF) + random.uniform(0, 0.2)
                        time.sleep(backoff)
                    else:
                        print(f"[ERR] Agotados reintentos: {nodo}.{linea} = {nuevo_valor_bool}")
                        append_measure(nodo=nodo, dt_s=0, command=False)
                        _fallback_set_opposite(nodo, bool(nuevo_valor_bool))
                        return

        finally:
            # pase lo que pase, reanuda el cliente PLC4X
            try:
                print("[MAINT] Reanudando plc4xclient…")
                start_plc4x()
            except Exception as e:
                print(f"[ERR] No se pudo iniciar plc4xclient: {e}")

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
                        last_seen[tag] = val  # actualiza baseline igual
                        continue
                    last_emit_ts[tag] = now_s

                    # log básico
                    print(f"[{datetime.now().isoformat(timespec='seconds')}] "
                          f"TAG={tag}  ADDR={addr}  NEW_VALUE={val}  (raw={raw})")

                    # actualiza baseline
                    last_seen[tag] = val

                    # dispara flancos
                    if prev is None:
                        continue  # primera muestra: no dispares
                    if prev is False and val is True:
                        on_holding_change(tag, True, addr, TAG_TO_ACTION)
                    elif prev is True and val is False:
                        on_holding_change(tag, False, addr, TAG_TO_ACTION)

        except PyMongoError as e:
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
