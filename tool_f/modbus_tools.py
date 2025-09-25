# tool_f/modbus_tools.py
from __future__ import annotations
import json
from typing import Any, Dict
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

IP, PORT, TIMEOUT = "192.168.1.200", 502, 3.0
RUTA_JSON = "mapaIEEE13.json"

# -------- Utils --------
def _load_cfg(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _slave_y_addr(cfg: Dict[str, Any], nodo: str, señal: str) -> tuple[int, int]:
    nd = cfg["Nodos"][nodo]              # KeyError si falta
    slave = int(nd["slave"])
    dir_1b = nd[señal]                   # puede ser int o null
    if dir_1b is None:
        raise KeyError(f"La señal '{señal}' en {nodo} no tiene dirección (null) en el JSON.")
    addr0 = int(dir_1b) # - 1          # 0-based
    return slave, addr0

def _with_client(fn, *, host=IP, port=PORT, timeout=TIMEOUT):
    c = ModbusTcpClient(host, port=port, timeout=timeout, strict=False, retry_on_empty=True, retries=3)
    if not c.connect():
        raise RuntimeError("No conecta")
    try:
        return fn(c)
    finally:
        c.close()

# -------- PLC (coils) --------
def escribir_plc(
    nodo: str,
    señal: str,
    valor: bool,
    *,
    ruta_json: str = RUTA_JSON,
    host: str = IP,
    port: int = PORT,
    timeout: float = TIMEOUT,
    verificar: bool = True,
    verify_retries: int = 3,
) -> bool:
    """Escribe una coil (FC5) usando nodo/señal del JSON. Verifica opcionalmente leyendo."""
    cfg = _load_cfg(ruta_json)
    slave, addr0 = _slave_y_addr(cfg, nodo, señal)

    wr = _with_client(lambda c: c.write_coil(address=addr0, value=valor, slave=slave),
                      host=host, port=port, timeout=timeout)
    if not wr or wr.isError():
        raise RuntimeError(f"WRITE coil error @({nodo}.{señal}): {wr}")

    if not verificar:
        return True

    for _ in range(verify_retries):
        rr = _with_client(lambda c: c.read_coils(address=addr0, count=1, slave=slave),
                          host=host, port=port, timeout=timeout)
        if rr and not rr.isError() and bool(rr.bits[0]) == bool(valor):
            return True
    raise RuntimeError(f"No se pudo verificar {nodo}.{señal}={valor} tras {verify_retries} intentos")

def leer_plc(
    nodo: str,
    señal: str,
    *,
    ruta_json: str = RUTA_JSON,
    host: str = IP,
    port: int = PORT,
    timeout: float = TIMEOUT,
) -> bool:
    """Lee una coil (FC1) usando nodo/señal del JSON."""
    cfg = _load_cfg(ruta_json)
    slave, addr0 = _slave_y_addr(cfg, nodo, señal)
    rr = _with_client(lambda c: c.read_coils(address=(addr0 - 1), count=1, slave=slave),
                      host=host, port=port, timeout=timeout)
    if not rr or rr.isError():
        raise RuntimeError(f"READ coil error @({nodo}.{señal}): {rr}")
    return bool(rr.bits[0])

# -------- Medidores (FC3) --------
def leer_medidor(
    medidor: str,
    parametro: str,
    *,
    ruta_json: str = RUTA_JSON,
    host: str = IP,
    port: int = PORT,
    timeout: float = TIMEOUT,
    decode: str | None = None,
    return_unit: bool = False,
):
    """
    Lee 'parametro' del 'medidor' vía Holding Registers (FC3) usando el mapa del JSON.

    JSON soportado:
      "ParamX": 3030
      "ParamX": {"addr": 3030, "unit": "V", "decode": "float32", "scale": 1.0}
    """
    cfg = _load_cfg(ruta_json)

    slave = int(cfg["Medidores"][medidor]["slave"])
    entry = cfg["Registros_Medidor"][parametro]  # KeyError si no existe

    if isinstance(entry, dict):
        reg_1b = int(entry["addr"])
        unit = entry.get("unit")
        dec_json = (entry.get("decode") or "").lower() or None
        scale = float(entry.get("scale", 1.0))
    else:
        reg_1b = int(entry)
        unit = None
        dec_json = None
        scale = 1.0

    addr0 = reg_1b - 1
    pname = parametro.lower()
    dec = (decode or dec_json or ("u32" if "energia" in pname else "float32")).lower()

    c = ModbusTcpClient(host, port=port, timeout=timeout, strict=False)
    if not c.connect():
        raise RuntimeError("No conecta al servidor Modbus")
    try:
        if dec == "float32":
            rr = c.read_holding_registers(address=addr0, count=2, slave=slave)
            if not rr or rr.isError():
                raise RuntimeError(f"FC3 error HR[{reg_1b}]..+1: {rr}")
            decod = BinaryPayloadDecoder.fromRegisters(
                rr.registers, byteorder=Endian.BIG, wordorder=Endian.BIG
            )
            val = float(decod.decode_32bit_float())
        elif dec == "u16":
            rr = c.read_holding_registers(address=addr0, count=1, slave=slave)
            if not rr or rr.isError():
                raise RuntimeError(f"FC3 error HR[{reg_1b}]: {rr}")
            val = int(rr.registers[0] & 0xFFFF)
        elif dec == "u32":
            rr = c.read_holding_registers(address=addr0, count=2, slave=slave)
            if not rr or rr.isError():
                raise RuntimeError(f"FC3 error HR[{reg_1b}]..+1: {rr}")
            decod = BinaryPayloadDecoder.fromRegisters(
                rr.registers, byteorder=Endian.BIG, wordorder=Endian.BIG
            )
            val = int(decod.decode_32bit_uint())
        else:
            raise ValueError("decode debe ser 'float32' | 'u16' | 'u32'")
    finally:
        c.close()

    val = val * scale
    return (val, unit) if return_unit else val
