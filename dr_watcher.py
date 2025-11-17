#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dr_onchange_periodic_pt_hr.py  (Mongo-driven action, Meter-based verification with anti-stale guard)

PHASES
- NORMAL (Mongo): Read PT from Mongo. If outside hysteresis band -> perform ONE action (restore/shed),
  then start a 10 s pause and set await_verify=True.
- VERIFY (Meter): After the pause, read PT from the physical meter (leer_medidor). If still outside band -> perform ONE action,
  then pause 10 s and KEEP await_verify=True. If in-band -> clear await_verify and set prefer_meter_after_clear=True,
  which forces the next apparent Mongo breach to be rechecked by the meter before acting (prevents stale Mongo causing extra actions).

OTHER RULES
- Exactly ONE action per evaluation.
- 10 s hard pause after EVERY action (no PT reads during pause).
- No pre-read of HR before writing; Modbus connection opens only to write and closes immediately.
- Starts the PT reader automatically at boot if DR_STATE is already True.
"""

from __future__ import annotations
import time
import threading
from typing import Optional, List, Tuple, Dict

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymodbus.client import ModbusTcpClient

# ----------------- Config Mongo -----------------
MONGO_URI       = "mongodb://127.0.0.1:27017/"
DB_NAME         = "json_scada"
COL_RT          = "realtimeData"

# Tags de control
TAG_DR_STATE    = "DR_STATE"
TAG_P_TOT       = "NODO650_P_TOT"   # PT primario desde Mongo (fase NORMAL)
TAG_P_MIN       = "DR_Pmin"
TAG_P_MAX       = "DR_Pmax"
TAG_PRI_1       = "DR_PRI1"
TAG_PRI_2       = "DR_PRI2"
TAG_PRI_3       = "DR_PRI3"

# ----------------- Config Modbus (servidor local) -----------------
MB_HOST         = "127.0.0.1"
MB_PORT         = 1502
MB_UNIT         = 1
MB_TIMEOUT      = 2.0

# Mapea NODO -> índice 0-based del HR a escribir (U16 0/1)
NODE_TO_HR_IDX: dict[int, int] = {
    645: 20,   # 4x00021
    634: 36,
    646: 52,
    675: 87,
    611: 69,
    692: 100,
}

# Periodicidad de lectura (cuando no hay pausa activa)
READ_PERIOD_S       = 1.0
CONNECT_BACKOFF     = 2.0

# Histéresis relativa
HYST_REL            = 0.01   # 5%

# Pausa dura después de cada write
POST_CMD_PAUSE_S    = 20  # ajusta aquí la espera entre decisiones

# Valores de prioridad permitidos
ALLOWED_NODES = {634, 646, 645, 675, 611, 692}

# ----------------- Lector de Medidor (solo en fase VERIFY) -----------------
try:
    from tool_f.modbus_tools import leer_medidor, RUTA_JSON as MT_RUTA_JSON, IP as MT_IP, PORT as MT_PORT
    METER_AVAILABLE = True
except Exception:
    METER_AVAILABLE = False
    MT_RUTA_JSON = None
    MT_IP = None
    MT_PORT = None

METER_NAME      = "Medidor_150"
METER_PARAM_PT  = "Potencia_Activa_Total"
METER_TIMEOUT_S = 2.0  # ajustable


# -------------------------------------------
# -------- utilidades Mongo --------
def _extract_value(doc: dict) -> Optional[str]:
    if not doc:
        return None
    for k in ("value", "valueString", "valueJson", "lastValue"):
        if doc.get(k) is not None:
            return doc[k]
    return None

def read_tag_raw(db, tag: str):
    return _extract_value(
        db[COL_RT].find_one(
            {"tag": tag},
            {"value":1, "valueString":1, "valueJson":1, "lastValue":1, "_id":0, "tag":1}
        )
    )

def _as_bool(v) -> Optional[bool]:
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return v != 0
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("1","true","on","yes","y"): return True
        if t in ("0","false","off","no","n"): return False
    try:
        return bool(int(v))
    except Exception:
        return None

def _as_float(v) -> Optional[float]:
    try:
        if v is None: return None
        return float(v)
    except Exception:
        return None

def read_tag_float(db, tag: str) -> Optional[float]:
    return _as_float(read_tag_raw(db, tag))

def _norm_pri_val(raw) -> Optional[int]:
    """634 / '634' / 634.0 -> 634 si está permitido."""
    if raw is None:
        return None
    try:
        n = int(round(float(str(raw).strip())))
        return n if n in ALLOWED_NODES else None
    except Exception:
        return None


# -------------------------------------------
# -------- PT helpers --------
def read_pt_mongo(db) -> Optional[float]:
    """PT desde Mongo (fase NORMAL)."""
    return read_tag_float(db, TAG_P_TOT)

def read_pt_meter_verify() -> Optional[float]:
    """PT desde el medidor (fase VERIFY). Conservador: si falla, devolvemos None y no actuamos."""
    if not METER_AVAILABLE:
        return None
    try:
        val = leer_medidor(
            medidor=METER_NAME,
            parametro=METER_PARAM_PT,
            ruta_json=MT_RUTA_JSON,
            host=MT_IP,
            port=MT_PORT,
            timeout=METER_TIMEOUT_S,
            decode="float32",
            return_unit=False
        )
        return _as_float(val)
    except Exception as e:
        print(f"[PT] Meter read (VERIFY) failed: {e}")
        return None


# -------------------------------------------
# -------- utilidades Modbus (conexión por write) --------
def _write_hr_bool_once(hr_idx0: int, desired: bool) -> bool:
    """
    Escribe 0/1 (U16) en HR idx0 (zero_mode=True).
    - Conecta, escribe, cierra SIEMPRE.
    - No hace pre-lectura.
    """
    cli = ModbusTcpClient(
        MB_HOST, port=MB_PORT, timeout=MB_TIMEOUT,
        strict=False, retry_on_empty=True, retries=2
    )
    if not cli.connect():
        print("[DR] ERROR: no conecta con el servidor Modbus local")
        return False
    try:
        wr = cli.write_register(address=int(hr_idx0), value=(1 if desired else 0), slave=MB_UNIT)
        if not wr or wr.isError():
            print(f"[DR] ERROR WRITE U16 HR[idx0={hr_idx0}]: {wr}")
            return False
        return True
    finally:
        cli.close()


# -------------------------------------------
# -------- lector periódico de PT + lógica DR --------
class PeriodicPTReader:
    """DR engine con dos fases: NORMAL (Mongo) y VERIFY (Medidor)."""
    def __init__(self, db, period_s: float = 1.0):
        self.db = db
        self.period_s = max(0.1, float(period_s))
        self._stop_evt = threading.Event()
        self._thr: Optional[threading.Thread] = None
        self._pause_until: float = 0.0  # bloquea lectura de P tras cada write

        # cache de última orden enviada por nodo (True=ON, False=OFF)
        self._last_cmd_desired: Dict[int, bool] = {}

        # Estado de verificación
        self._await_verify: bool = False        # True => próxima medición POR MEDIDOR
        self._last_action: Optional[str] = None # "shed"/"restore" (debug)

        # Anti-stale guard: tras limpiar verificación, preferir medir con medidor antes de actuar por Mongo
        self._prefer_meter_after_clear: bool = False

    def start(self):
        if self._thr and self._thr.is_alive():
            return
        self._stop_evt.clear()
        self._thr = threading.Thread(target=self._run, name="PeriodicPTReader", daemon=True)
        self._thr.start()
        print("[DR] PT Reader: STARTED")

    def stop(self):
        if not self._thr:
            return
        self._stop_evt.set()
        self._thr.join(timeout=self.period_s * 3.0)
        print("[DR] PT Reader: STOPPED")

    # -------- helper: enviar comando HR y pausar 10s --------
    def _send_node_cmd(self, node: int, desired: bool) -> bool:
        if node not in NODE_TO_HR_IDX:
            print(f"[DR] (WARN) Nodo {node} sin HR mapeado; completa NODE_TO_HR_IDX.")
            return False

        hr_idx0 = NODE_TO_HR_IDX[node]
        ok = _write_hr_bool_once(hr_idx0, desired)
        state = "ON" if desired else "OFF"
        if ok:
            print(f"[DR] HR WRITE: node={node} idx0={hr_idx0} (4x{hr_idx0+1:05d}) <- {1 if desired else 0}  ({state})")
            self._last_cmd_desired[node] = desired
        else:
            print(f"[DR] ERROR escribiendo HR node={node} idx0={hr_idx0} ({state})")

        # Pausa dura antes de volver a leer P
        self._pause_until = time.time() + POST_CMD_PAUSE_S
        return True  # se intentó (independiente de ok)

    # -------- prioridades --------
    def _get_priorities(self) -> List[Tuple[str, int]]:
        raws = [("Pri1", read_tag_raw(self.db, TAG_PRI_1)),
                ("Pri2", read_tag_raw(self.db, TAG_PRI_2)),
                ("Pri3", read_tag_raw(self.db, TAG_PRI_3))]

        seen, res = set(), []
        for label, raw in raws:
            n = _norm_pri_val(raw)
            if n is None:
                continue
            if n in seen:
                print(f"[DR] (WARN) {label} dup node {n}, skipping.")
                continue
            res.append((label, n))
            seen.add(n)

        print("[DR] Prioridades: " + ", ".join(f"{lbl}={n}" for lbl, n in res))
        return res

    # -------- acciones --------
    def _shed_once(self, order_p3_p2_p1: List[Tuple[str,int]]) -> bool:
        for label, node in order_p3_p2_p1:
            if self._last_cmd_desired.get(node) is False:
                continue
            self._send_node_cmd(node, False)
            print(f"[DR] Shed: {label} (node={node}) -> OFF (intentado)")
            return True
        return False

    def _restore_once(self, order_p1_p2_p3: List[Tuple[str,int]]) -> bool:
        for label, node in order_p1_p2_p3:
            if self._last_cmd_desired.get(node) is True:
                continue
            self._send_node_cmd(node, True)
            print(f"[DR] Restore: {label} (node={node}) -> ON (intentado)")
            return True
        return False

    # -------- evaluación central por valor y fase --------
    def _on_power_sample_value(self, pt_value: float, source: str):
        """Decide basado en un PT concreto y la fase actual."""
        pmin = read_tag_float(self.db, TAG_P_MIN)
        pmax = read_tag_float(self.db, TAG_P_MAX)
        if pmin is None or pmax is None:
            print("[DR] ERROR: DR_Pmin/DR_Pmax missing or not numeric.")
            return

        pmin_lo = pmin * (1.0 - HYST_REL)
        pmax_hi = pmax * (1.0 + HYST_REL)

        pris = self._get_priorities()
        if not pris:
            print("[DR] WARN: No hay prioridades válidas (DR_PRI1..3).")
            return

        order_restore = pris                 # Pri1 -> Pri2 -> Pri3
        order_shed    = list(reversed(pris)) # Pri3 -> Pri2 -> Pri1

        print(f"[DR] PT_{source}={pt_value:.3f} kW | Pmin={pmin:.3f} (lo={pmin_lo:.3f}) | "
              f"Pmax={pmax:.3f} (hi={pmax_hi:.3f}) | phase={'VERIFY' if self._await_verify else 'NORMAL'}")

        below = pt_value < pmin_lo
        above = pt_value > pmax_hi

        if not below and not above:
            # Dentro de banda
            if self._await_verify:
                self._await_verify = False
                self._last_action = None
                self._prefer_meter_after_clear = True  # <-- clave: preferir medidor en próximo posible breach
                print("[DR] In-band on meter verification -> verification cleared (prefer meter next).")
            return

        # Fuera de banda: una sola acción por evaluación
        # Anti-stale: si acabamos de limpiar verificación, no actuar en Mongo sin verificar con medidor
        if source == "mongo" and self._prefer_meter_after_clear:
            self._await_verify = True
            print("[DR] Mongo shows breach but prefer_meter_after_clear=True -> switching to VERIFY (meter) before acting.")
            return

        if below:
            acted = self._restore_once(order_restore)
            if acted:
                self._last_action = "restore"
                self._pause_until = time.time() + POST_CMD_PAUSE_S
                self._await_verify = True
                self._prefer_meter_after_clear = False  # ya iniciamos una nueva escalada
            else:
                print("[DR] Restore: nada para encender (todos ON o sin HR).")
            return

        if above:
            acted = self._shed_once(order_shed)
            if acted:
                self._last_action = "shed"
                self._pause_until = time.time() + POST_CMD_PAUSE_S
                self._await_verify = True
                self._prefer_meter_after_clear = False
            else:
                print("[DR] Shed: nada para apagar (todos OFF o sin HR).")
            return

    # -------- ciclo principal --------
    def _run(self):
        while not self._stop_evt.is_set():
            now = time.time()

            # Respetar la pausa dura tras cada write
            if now < self._pause_until:
                time.sleep(min(0.1, self._pause_until - now))
                continue

            try:
                if self._await_verify:
                    # --- Fase VERIFY: medir con MEDIDOR ---
                    pt_meter = read_pt_meter_verify()
                    if pt_meter is None:
                        # Conservador: si no hay medidor, no actuamos y mantenemos verify
                        print("[DR] Verify: meter unavailable -> skipping action (await_verify remains).")
                    else:
                        self._on_power_sample_value(pt_value=pt_meter, source="meter")
                else:
                    # --- Fase NORMAL: medir con MONGO ---
                    pt_mongo = read_pt_mongo(self.db)
                    if pt_mongo is None:
                        print("[DR] PT Mongo no disponible.")
                    else:
                        # Si está fuera de banda en NORMAL, aquí se actúa UNA vez y se entra en VERIFY
                        self._on_power_sample_value(pt_value=pt_mongo, source="mongo")

            except Exception as e:
                print(f"[DR] Error en ciclo DR: {e}")
            finally:
                # Sueño “interrumpible” entre chequeos (NO es la pausa de 10 s)
                remaining = self.period_s
                while remaining > 0 and not self._stop_evt.is_set():
                    step = min(0.1, remaining)
                    time.sleep(step)
                    remaining -= step


# -------------------------------------------
# -------- watcher de cambios en DR_STATE --------
def watch_dr_state_changes():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = client[DB_NAME]
    reader = PeriodicPTReader(db=db, period_s=READ_PERIOD_S)

    pipeline = [
        {"$match": {"operationType": {"$in": ["update", "replace"]}}},
        {"$match": {"fullDocument.tag": TAG_DR_STATE}},
        {"$match": {
            "$or": [
                {"updateDescription.updatedFields.value": {"$exists": True}},
                {"updateDescription.updatedFields.valueString": {"$exists": True}},
                {"updateDescription.updatedFields.valueJson": {"$exists": True}},
                {"updateDescription.updatedFields.lastValue": {"$exists": True}},
            ]
        }},
    ]

    # Arranque según estado actual
    current = _as_bool(read_tag_raw(db, TAG_DR_STATE))
    print(f"[BOOT] DR_STATE initial = {current}")
    if current:
        reader.start()

    while True:
        try:
            print("[WATCH] Esperando CAMBIOS reales en DR_STATE… (Ctrl+C para salir)")
            with db[COL_RT].watch(pipeline=pipeline, full_document="updateLookup") as stream:
                for ev in stream:
                    fd  = ev.get("fullDocument") or {}
                    upd = (ev.get("updateDescription") or {}).get("updatedFields", {})

                    # Solo avances que realmente cambian el valor
                    if ev.get("operationType") == "update" and not any(
                        k in upd for k in ("value", "valueString", "valueJson", "lastValue")
                    ):
                        continue

                    raw = _extract_value(fd)
                    new_state = _as_bool(raw)
                    if new_state is None or new_state == current:
                        continue
                    current = new_state

                    ts = time.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{ts}] DR_STATE -> {new_state} (raw={raw})")

                    if new_state:
                        reader.start()
                    else:
                        reader.stop()

        except PyMongoError as e:
            print(f"[WATCH] Change Stream error: {e}  (reintento en {CONNECT_BACKOFF}s)")
            time.sleep(CONNECT_BACKOFF)
        except KeyboardInterrupt:
            print("\n[SIGNAL] Interrumpido por el usuario. Saliendo…")
            break
        except Exception as e:
            print(f"[WATCH] Error inesperado: {e}  (reintento en {CONNECT_BACKOFF}s)")
            time.sleep(CONNECT_BACKOFF)

    # Limpieza
    try: reader.stop()
    except Exception: pass
    try: client.close()
    except Exception: pass
    print("[EXIT] dr_onchange_periodic_pt_hr terminado.")


# -------------------------------------------
if __name__ == "__main__":
    watch_dr_state_changes()
