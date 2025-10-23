#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dr_onchange_periodic_pt_hr.py
- Cambia cargas vía HR (0/1) en un servidor Modbus local SOLO cuando DR lo requiere.
- Conexión Modbus: se abre únicamente al enviar el comando, se cierra inmediatamente.
- Después de cada comando, pausa 10 s antes de volver a leer PT.
- Lógica: si PT > Pmax(1+histéresis) → deslastra en orden de prioridad (Pri3→Pri2→Pri1).
         si PT < Pmin(1−histéresis) → restaura en orden (Pri1→Pri2→Pri3).
- No pre-lee HR antes de escribir (cumple tu requerimiento).
"""

from __future__ import annotations
import time
import threading
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymodbus.client import ModbusTcpClient

# ----------------- Config Mongo -----------------
MONGO_URI       = "mongodb://127.0.0.1:27017/"
DB_NAME         = "json_scada"
COL_RT          = "realtimeData"

# Tags de control
TAG_DR_STATE    = "DR_STATE"
TAG_P_TOT       = "NODO650_P_TOT"   # PT leído desde Mongo
TAG_P_MIN       = "DR_Pmin"
TAG_P_MAX       = "DR_Pmax"
TAG_PRI_1       = "DR_PRI1"
TAG_PRI_2       = "DR_PRI2"
TAG_PRI_3       = "DR_PRI3"

# ----------------- Config Modbus (tu servidor) -----------------
MB_HOST         = "127.0.0.1"
MB_PORT         = 1502
MB_UNIT         = 1
MB_TIMEOUT      = 2.0

# Mapea NODO -> índice 0-based del HR a escribir (U16 0/1).
# EJEMPLO confirmado por tus logs: NODO 645 -> idx0=17 (4x00018)
NODE_TO_HR_IDX: dict[int, int] = {
    645: 17,   # 4x00018
    # 634: <idx>,
    # 646: <idx>,
    # 675: <idx>,
    # 611: <idx>,
    # 692: <idx>,
}

# Periodicidad de lectura de PT mientras DR=ON
READ_PERIOD_S       = 1.0
CONNECT_BACKOFF     = 2.0

# Histéresis
HYST_REL            = 0.05   # 5%

# Pausa dura después de cada write (tu requerimiento)
POST_CMD_PAUSE_S    = 10.0

# Valores de prioridad permitidos
ALLOWED_NODES = {634, 646, 645, 675, 611, 692}

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
            {"value":1, "valueString":1, "valueJson":1, "lastValue":1}
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
    """Lee PT (tag Mongo) periódicamente y aplica lógica DR.
       Acciones: escribir HR (0/1) conectando SOLO al enviar y cerrando luego.
    """
    def __init__(self, db, period_s: float = 1.0):
        self.db = db
        self.period_s = max(0.1, float(period_s))
        self._stop_evt = threading.Event()
        self._thr: Optional[threading.Thread] = None
        self._pause_until: float = 0.0  # bloquea lectura de P tras cada write

        # cache de última orden enviada por nodo (para no repetir y avanzar al siguiente)
        # True = ON ordenado, False = OFF ordenado
        self._last_cmd_desired: Dict[int, bool] = {}

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
            # memoriza la orden para no repetirla innecesariamente
            self._last_cmd_desired[node] = desired
        else:
            print(f"[DR] ERROR escribiendo HR node={node} idx0={hr_idx0} ({state})")

        # Pausa dura antes de volver a leer P (tu requerimiento)
        self._pause_until = time.time() + POST_CMD_PAUSE_S
        return True  # se intentó (independiente de ok)

    # -------- prioridades --------
    def _get_priorities(self) -> List[Tuple[str, int]]:
        raw1 = read_tag_raw(self.db, TAG_PRI_1)
        raw2 = read_tag_raw(self.db, TAG_PRI_2)
        raw3 = read_tag_raw(self.db, TAG_PRI_3)

        n1 = _norm_pri_val(raw1)
        n2 = _norm_pri_val(raw2)
        n3 = _norm_pri_val(raw3)

        print(f"[DR] Prioridades actuales: Pri1={raw1}->{n1}, Pri2={raw2}->{n2}, Pri3={raw3}->{n3}")

        res: List[Tuple[str, int]] = []
        if n1 is not None: res.append(("Pri1", n1))
        if n2 is not None: res.append(("Pri2", n2))
        if n3 is not None: res.append(("Pri3", n3))
        return res

    # -------- acciones --------
    def _shed_once(self, order_p3_p2_p1: List[Tuple[str,int]]) -> bool:
        """
        Deslastra UNA carga por ciclo (para respetar tu pausa de 10 s).
        - Orden: Pri3 -> Pri2 -> Pri1.
        - No pre-lee HR; usa el cache de última orden para saltar nodos ya ordenados OFF.
        """
        for label, node in order_p3_p2_p1:
            # si ya ordenamos OFF a este nodo, salta al siguiente
            if self._last_cmd_desired.get(node) is False:
                continue
            self._send_node_cmd(node, False)
            print(f"[DR] Shed: {label} (node={node}) -> OFF (intentado)")
            return True
        return False

    def _restore_once(self, order_p1_p2_p3: List[Tuple[str,int]]) -> bool:
        """
        Restaura UNA carga por ciclo.
        - Orden: Pri1 -> Pri2 -> Pri3.
        - Sin pre-lectura; usa cache para no repetir ON al mismo nodo.
        """
        for label, node in order_p1_p2_p3:
            if self._last_cmd_desired.get(node) is True:
                continue
            self._send_node_cmd(node, True)
            print(f"[DR] Restore: {label} (node={node}) -> ON (intentado)")
            return True
        return False

    # -------- decisión con histéresis --------
    def on_power_sample(self, pt_value: float):
        pmin = read_tag_float(self.db, TAG_P_MIN)
        pmax = read_tag_float(self.db, TAG_P_MAX)
        if pmin is None or pmax is None:
            print(f"[DR] Falta DR_Pmin/DR_Pmax (pmin={pmin}, pmax={pmax})")
            return

        pmin_lo = pmin * (1.0 - HYST_REL)
        pmax_hi = pmax * (1.0 + HYST_REL)

        pris = self._get_priorities()          # [('Pri1', n1), ('Pri2', n2), ('Pri3', n3)]
        order_restore = pris                    # Pri1 -> Pri2 -> Pri3
        order_shed    = list(reversed(pris))    # Pri3 -> Pri2 -> Pri1

        print(f"[DR] PT={pt_value:.3f} kW | Pmin={pmin} (lo={pmin_lo:.3f}) | "
              f"Pmax={pmax} (hi={pmax_hi:.3f}) | pris={pris}")

        # 1) PRIORIDAD: cumplir Pmin (si estamos POR DEBAJO, encendemos UNA carga)
        if pt_value < pmin_lo:
            acted = self._restore_once(order_restore)
            if not acted:
                print("[DR] Restore: nada para encender (sin HR mapeado o todos ya ordenados ON).")
            return  # importante: no evaluar Pmax en este ciclo

        # 2) Solo si Pmin ya se cumple, verificamos Pmax (si estamos POR ENCIMA, apagamos UNA carga)
        if pt_value > pmax_hi:
            acted = self._shed_once(order_shed)
            if not acted:
                print("[DR] Shed: nada para apagar (sin HR mapeado o todos ya ordenados OFF).")
        # 3) Dentro de banda: no actuar


    # -------- ciclo de lectura PT --------
    def _run(self):
        while not self._stop_evt.is_set():
            # Pausa tras un write (bloquea muestreo de P)
            now = time.time()
            if now < self._pause_until:
                time.sleep(min(0.1, self._pause_until - now))
                continue

            try:
                pt = read_tag_float(self.db, TAG_P_TOT)
                if pt is not None:
                    self.on_power_sample(pt)
                else:
                    print("[DR] PT tag no disponible/convertible")
            except Exception as e:
                print(f"[DR] Error leyendo PT desde Mongo: {e}")
            finally:
                # sueño “interrumpible”
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

    current: Optional[bool] = None
    while True:
        try:
            print("[WATCH] Esperando CAMBIOS reales en DR_STATE… (Ctrl+C para salir)")
            with db[COL_RT].watch(pipeline=pipeline, full_document="updateLookup") as stream:
                for ev in stream:
                    fd  = ev.get("fullDocument") or {}
                    upd = (ev.get("updateDescription") or {}).get("updatedFields", {})

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
