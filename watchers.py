#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Watcher de holdings 4x en json_scada.realtimeData.

- Observa cambios en tags (WATCH_TAGS) definidos en tags_ctrl.json
- Traducidos via TAG_TO_ACTION -> (Nodo_X, linea_yyy) y ejecuta escribir_plc()
- Si escribir_plc falla tras reintentos, usa fallback: escribe el **opuesto** en HR[idx0]
  tomado de hr_map.json, y activa ignorancia (cooldown) del tag para NO entrar en bucle.
- Anti-eco: "skip una vez" para suprimir el evento que nosotros mismos generamos.
"""

from tool_f.modbus_tools import leer_medidor, escribir_plc, leer_plc
from plc4_ctrl import stop_plc4x, start_plc4x
import revive

import os, json, time, random, struct, re, threading
from pathlib import Path
from datetime import datetime
from typing import Tuple, Sequence, Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pymodbus.client import ModbusTcpClient

# =========================
# Parámetros
# =========================
PLC4X_WINDOW_LOCK      = threading.Lock()  # evita ventanas solapadas
PRE_WRITE_PAUSE_S      = 0.6               # 300–800 ms recomendado
POST_WRITE_VERIFY_S    = 0.12              # espera breve antes de leer/verificar (si aplica)
MAX_RETRIES            = 8
TIMEOUT_S              = 3.0               # segundos por intento escribir_plc
BASE_BACKOFF           = 0.25              # s (exponencial con jitter)
MAX_BACKOFF            = 2.0               # s

# Cooldown / suppress windows
SUCCESS_MINI_COOLDOWN_S = 2.0              # anti rebote inmediato tras éxito
FAIL_COOLDOWN_S         = 20.0             # ventana para ignorar el tag tras fallar + fallback
ENTER_HANDLER_IGNORE_S  = 6.0              # suprime eventos durante la ventana stop->write->start

# =========================
# Paths
# =========================
BASE_DIR = Path(__file__).resolve().parent
MEASURES_PATH = BASE_DIR / "measures_time.json"
MAPPING_PATH  = BASE_DIR / "tags_ctrl.json"   # contiene TAG_TO_ACTION y WATCH_TAGS
HR_MAP_PATH   = BASE_DIR / "hr_map.json"      # {tag: {table, addr_1b, idx0, dtype}}

# =========================
# Modbus (fallback directo)
# =========================
MODBUS_HOST = "192.168.1.200"
MODBUS_PORT = 502
MODBUS_TIMEOUT = 3.0
MB_HOST = "127.0.0.1"     # fallback directo (gateway local)
MB_PORT = 1502
MB_UNIT = 1               # ajusta al Unit-ID real del PLC/gateway

# Endianness (para funciones float, si las usas)
HR_BYTEORDER = "big"      # "big"|"little": bytes dentro de cada U16
HR_WORDORDER = "big"      # "big"|"little": orden de palabras (HiWord primero = "big")

def _endianness() -> Tuple[str, str]:
    return HR_BYTEORDER, HR_WORDORDER

def float32_to_registers(value: float) -> Tuple[int, int]:
    border, worder = _endianness()
    be_bytes = struct.pack(">f", float(value))
    hi, lo = be_bytes[:2], be_bytes[2:]
    word_hi, word_lo = (hi, lo) if worder == "big" else (lo, hi)

    def b2u16(b: bytes, border: str) -> int:
        return int.from_bytes(b, byteorder=("big" if border == "big" else "little"), signed=False)

    return b2u16(word_hi, border), b2u16(word_lo, border)

def registers_to_float32(regs: Sequence[int]) -> float:
    assert len(regs) >= 2, "Se requieren 2 registros para float32"
    border, worder = _endianness()
    def u16_to_bytes(x: int, border: str) -> bytes:
        return int(x & 0xFFFF).to_bytes(2, byteorder=("big" if border == "big" else "little"), signed=False)
    b_hi = u16_to_bytes(regs[0], border)
    b_lo = u16_to_bytes(regs[1], border)
    be_bytes = (b_hi + b_lo) if worder == "big" else (b_lo + b_hi)
    return struct.unpack(">f", be_bytes)[0]

def write_hr_u16(*, host=MODBUS_HOST, port=MODBUS_PORT, slave: int, reg_1b: int, value: int, timeout: float = MODBUS_TIMEOUT) -> None:
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
    addr0 = int(reg__1b) - 1
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
# Mongo
# =========================
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME   = "json_scada"
COL_RT    = "realtimeData"

# =========================
# Estado/ayudas del watcher
# =========================
# Baseline de valor por tag
BASELINE_LOCK = threading.Lock()
BASELINE: dict[str, object] = {}        # tag -> último valor normalizado

def set_baseline(tag: str, value):
    with BASELINE_LOCK:
        BASELINE[tag] = value

def get_baseline(tag: str):
    with BASELINE_LOCK:
        return BASELINE.get(tag, None)

# "Skip una vez" (suprime el próximo evento si coincide el valor)
SKIP_LOCK = threading.Lock()
SKIP_ONCE: dict[str, Optional[bool]] = {}  # tag -> None (skip cualquier valor 1 vez) | True/False (skip solo ese valor 1 vez)

def set_skip_once(tag: str, value_bool_or_none: Optional[bool]):
    with SKIP_LOCK:
        SKIP_ONCE[tag] = value_bool_or_none

def should_skip_once(tag: str, value_bool: bool) -> bool:
    with SKIP_LOCK:
        rec = SKIP_ONCE.get(tag, None)
        if rec is None:
            return False
        # rec = None => saltar 1 vez cualquier valor
        # rec = True/False => saltar solo si coincide
        if (rec is None) or (isinstance(rec, bool) and bool(value_bool) == rec):
            SKIP_ONCE.pop(tag, None)
            return True
        return False

# Ignorar (cooldown) cualquier evento del tag hasta un tiempo
IGNORE_LOCK = threading.Lock()
IGNORE_UNTIL: dict[str, float] = {}     # tag -> epoch segundos

def set_ignore(tag: str, seconds: float) -> None:
    with IGNORE_LOCK:
        IGNORE_UNTIL[tag] = time.time() + float(seconds)

def is_ignored(tag: str) -> bool:
    now = time.time()
    with IGNORE_LOCK:
        ts = IGNORE_UNTIL.get(tag, 0.0)
        if now <= ts:
            return True
        if ts:
            IGNORE_UNTIL.pop(tag, None)
        return False

# Métricas
MEASURES_LOCK = threading.Lock()
def append_measure(nodo: str, dt_s: float, command: bool) -> None:
    rec = {
        "nodo": str(nodo),
        "dt_s": round(float(dt_s), 6),
        "command": bool(command),
    }
    with MEASURES_LOCK:
        try:
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
            MEASURES_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"[WARN] No se pudo guardar measures_time: {e}")

# Utilidades varias
def as_boolish(v):
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
    return v

def extract_value(doc):
    for k in ("lastValue", "value", "valueString", "valueJson"):
        if doc.get(k) is not None:
            return doc[k]
    return None

def call_with_timeout(func, *args, timeout=TIMEOUT_S, **kwargs):
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(func, *args, **kwargs)
        return fut.result(timeout=timeout)

# =========================
# Mapas de tags
# =========================
def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

_HR_MAP = _load_json(HR_MAP_PATH)       # tag -> {table, addr_1b, idx0, dtype}

def _write_bool_by_tag_idx0(tag: str, desired: bool) -> bool:
    info = _HR_MAP.get(tag)
    if not info or info.get("table") != "4x":
        print(f"[MAP] Tag '{tag}' no tiene 4x en hr_map.json")
        return False
    idx0 = int(info["idx0"])
    cli = ModbusTcpClient(MB_HOST, port=MB_PORT, timeout=MODBUS_TIMEOUT,
                          strict=False, retry_on_empty=True, retries=2)
    if not cli.connect():
        print(f"[HR] connect FAIL {MB_HOST}:{MB_PORT} (tag={tag})")
        return False
    try:
        wr = cli.write_register(address=idx0, value=(1 if desired else 0), slave=MB_UNIT)
        if not wr or wr.isError():
            print(f"[HR] WRITE ERR tag={tag} idx0={idx0} (4x{idx0+1:05d}): {wr}")
            return False
        print(f"[HR] OK tag={tag} idx0={idx0} (4x{idx0+1:05d}) <- {1 if desired else 0}")
        return True
    finally:
        cli.close()

# =========================
# Carga mapping principal
# =========================
def load_mapping(json_path: Path):
    if not json_path.exists():
        raise FileNotFoundError(f"No encuentro el archivo de mapeo: {json_path}")
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"{json_path.name} no es JSON válido: {e}")

    tag_to_action_raw = data.get("TAG_TO_ACTION", {})
    watch_tags = list(data.get("WATCH_TAGS", []))

    tag_to_action = {}
    for tag, pair in tag_to_action_raw.items():
        if (not isinstance(pair, (list, tuple))) or len(pair) != 2:
            raise ValueError(f"TAG_TO_ACTION['{tag}'] debe ser [\"Nodo_X\", \"linea_xxx\"], recibido: {pair}")
        nodo, linea = pair
        tag_to_action[tag] = (str(nodo), str(linea))

    missing = [t for t in watch_tags if t not in tag_to_action]
    if missing:
        print(f"[WARN] WATCH_TAGS sin acción definida en TAG_TO_ACTION: {missing}")

    return tag_to_action, watch_tags

# =========================
# Handler de cambios
# =========================
def on_holding_change(tag_holding: str, nuevo_valor_bool: bool, addr: str, TAG_TO_ACTION: dict):
    action = TAG_TO_ACTION.get(tag_holding)
    if not action:
        print(f"[SKIP] Sin mapeo para {tag_holding}")
        return
    nodo, linea = action

    # Durante toda la ventana stop->write->start ignoramos eventos del mismo tag
    set_ignore(tag_holding, ENTER_HANDLER_IGNORE_S)

    with PLC4X_WINDOW_LOCK:
        t0 = time.perf_counter()
        print("[MAINT] Deteniendo plc4xclient para ventana de mando…")
        stop_plc4x()

        try:
            time.sleep(PRE_WRITE_PAUSE_S)

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    ok = call_with_timeout(
                        escribir_plc, nodo, linea, bool(nuevo_valor_bool),
                        timeout=TIMEOUT_S
                    )
                    dt = time.perf_counter() - t0

                    # “Ping” opcional al PLC4X
                    revive.ping_medidores_hr3036(
                        ruta_json="mapaIEEE13.json",
                        host=MODBUS_HOST,
                        port=MODBUS_PORT,
                        timeout=2.0,
                        prefer_param="VLN_AVG",
                    )
                    time.sleep(POST_WRITE_VERIFY_S)

                    if ok:
                        append_measure(nodo=nodo, dt_s=dt, command=True)
                        print(f"[CMD] {tag_holding}({addr}) -> {nodo}.{linea} = {nuevo_valor_bool} | ok={ok} | attempt={attempt} | dt_s={dt:.6f}")

                        # Fija baseline al valor final y suprime solo el PRÓXIMO eco (si llega)
                        set_baseline(tag_holding, bool(nuevo_valor_bool))
                        set_skip_once(tag_holding, bool(nuevo_valor_bool))
                        # Mini-cooldown para rebotes inmediatos del HMI
                        set_ignore(tag_holding, SUCCESS_MINI_COOLDOWN_S)
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
                        dt = time.perf_counter() - t0
                        append_measure(nodo=nodo, dt_s=dt, command=False)

                        # FALLBACK: forzar el opuesto por idx0, y NO entrar en bucle
                        opposite = (not bool(nuevo_valor_bool))
                        _write_bool_by_tag_idx0(tag_holding, desired=opposite)

                        # Baseline y "skip una vez" al opuesto para consumir el eco del fallback
                        set_baseline(tag_holding, opposite)
                        set_skip_once(tag_holding, opposite)

                        # Cooldown largo: ignora CUALQUIER evento del tag por un tiempo
                        set_ignore(tag_holding, FAIL_COOLDOWN_S)
                        time.sleep(0.2)
                        return

        finally:
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

    pipeline = [
        {"$match": {"operationType": {"$in": ["update", "replace"]}}},
        {"$match": {"fullDocument.tag": {"$in": WATCH_TAGS}}},
    ]

    # Anti-rebote temporal entre emisiones
    last_emit_ts = {}   # tag -> tiempo de última emisión (seg)
    debounce_ms = 100

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

                    # 1) Cooldown global por tag (éxito/fallo/en handler)
                    if is_ignored(tag):
                        set_baseline(tag, val)   # manten baseline actualizado
                        # print(f"[IGNORE] {tag} en cooldown, val={val}")
                        continue

                    # Normaliza a bool cuando aplique
                    is_bool_val = isinstance(val, bool)

                    # 2) Skip una vez: consume nuestro eco (éxito o fallback)
                    if is_bool_val and should_skip_once(tag, bool(val)):
                        set_baseline(tag, val)
                        # print(f"[SKIP-ONCE] {tag} -> {val}")
                        continue

                    # 3) Suprime “no cambios” de valor
                    prev = get_baseline(tag)
                    if prev == val:
                        continue

                    # 4) Anti-rebote por tiempo
                    now_s = time.time()
                    last_ts = last_emit_ts.get(tag, 0)
                    if (now_s - last_ts) * 1000.0 < debounce_ms:
                        set_baseline(tag, val)
                        continue
                    last_emit_ts[tag] = now_s

                    # Log
                    print(f"[{datetime.now().isoformat(timespec='seconds')}] "
                          f"TAG={tag}  ADDR={addr}  NEW_VALUE={val}  (raw={raw})")

                    # Actualiza baseline ANTES de disparar
                    set_baseline(tag, val)

                    # 5) Dispara flancos sólo si ambos son booleanos
                    if isinstance(prev, bool) and isinstance(val, bool):
                        if prev is False and val is True:
                            on_holding_change(tag, True, addr, TAG_TO_ACTION)
                        elif prev is True and val is False:
                            on_holding_change(tag, False, addr, TAG_TO_ACTION)
                    else:
                        # primera muestra o valor no-bool: no disparar
                        pass

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
