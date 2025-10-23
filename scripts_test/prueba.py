# lector_minimo.py
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import json

HOST, PORT, TIMEOUT = "192.168.1.200", 502, 3.0
RUTA_JSON = "mapaIEEE13.json"

def leer_medidor(medidor: str, parametro: str,
                 *, ruta_json: str = RUTA_JSON,
                 host: str = HOST, port: int = PORT, timeout: float = TIMEOUT,
                 decode: str | None = None) -> float | int:
    """
    Lee 'parametro' del 'medidor' vía Holding Registers (FC3) usando el mapa del JSON.
    - decode: "float32" | "u16" | "u32" | None
      Si None: usa decode del JSON si existe; si no, 'u32' si el nombre contiene 'energia',
      de lo contrario 'float32'.
    """
    # --- cargar JSON ---
    with open(ruta_json, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # --- resolver slave y dirección ---
    slave = int(cfg["Medidores"][medidor]["slave"])
    entry = cfg["Registros_Medidor"][parametro]  # KeyError si no existe (intencional)
    if isinstance(entry, dict):
        reg_1b = int(entry["addr"])
        dec_json = (entry.get("decode") or "").lower() or None
    else:
        reg_1b = int(entry)
        dec_json = None
    addr0 = reg_1b - 1

    # --- decidir decode final ---
    pname = parametro.lower()
    dec = (decode or dec_json or ("u32" if "energia" in pname else "float32")).lower()

    # --- conexión y lectura FC3 ---
    c = ModbusTcpClient(host, port=port, timeout=timeout, strict=False)
    if not c.connect():
        raise RuntimeError("No conecta al servidor Modbus")
    try:
        if dec == "float32":
            rr = c.read_holding_registers(address=addr0, count=2, slave=slave)
            if not rr or rr.isError():
                raise RuntimeError(f"FC3 error HR[{reg_1b}]..+1: {rr}")
            decod = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.BIG, wordorder=Endian.BIG)
            return float(decod.decode_32bit_float())
        elif dec == "u16":
            rr = c.read_holding_registers(address=addr0, count=1, slave=slave)
            if not rr or rr.isError():
                raise RuntimeError(f"FC3 error HR[{reg_1b}]: {rr}")
            return int(rr.registers[0] & 0xFFFF)
        elif dec == "u32":
            rr = c.read_holding_registers(address=addr0, count=2, slave=slave)
            if not rr or rr.isError():
                raise RuntimeError(f"FC3 error HR[{reg_1b}]..+1: {rr}")
            decod = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.BIG, wordorder=Endian.BIG)
            return int(decod.decode_32bit_uint())
        else:
            raise ValueError("decode debe ser 'float32' | 'u16' | 'u32'")
    finally:
        c.close()


# --- ejemplo rápido ---
if __name__ == "__main__":
    ia = leer_medidor("Medidor_132", "Corriente_B")                 # float32 FC3
    vb = leer_medidor("Medidor_132", "Voltaje_B")                   # float32 FC3
    ea = leer_medidor("Medidor_132", "Energia_Activa_Total")        # auto u32
    print("I_A:", ia, "V_B:", vb, "E_A:", ea)



