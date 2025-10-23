# pip install "pymodbus>=3.6"
import asyncio
import json
import struct
from pathlib import Path
from threading import Lock

from pymodbus.server import StartAsyncTcpServer
from pymodbus.datastore import ModbusServerContext, ModbusSlaveContext
from pymodbus.datastore.store import ModbusSequentialDataBlock

# -------- Config --------
HR_COUNT    = 200                         # HR 0..199  ↔  4x00001..4x00200
LISTEN_ADDR = ("127.0.0.1", 1502)
UNIT_ID     = 1
STORE_PATH  = Path("hr_store.json")      # persistencia de palabras 16-bit

# Tipado por índice 0-based (4x00001 = idx 0)
# - 'bool' → clamp 0/1 en escrituras
# - 'f32'  → dos HR consecutivos forman un float32 (no clamp)
# Tipado por índice 0-based (4x00001 = idx 0)
# Desde 113 hacia adelante, todos en pares f32:
TYPEMAP = {i: 'f32' for i in range(113, HR_COUNT - 1, 2)}


# Endianness para float32 (palabra alta/baja en big-endian de bytes)
BYTEORDER = ">"    # '>' big-endian bytes, '<' little-endian bytes
WORD_SWAP = False  # True si tu cliente invierte el orden de palabras

# -------- Utils --------
_store_lock = Lock()

def _to_u16(x) -> int:
    return max(0, min(0xFFFF, int(x) & 0xFFFF))

def _clamp_bool01(x) -> int:
    try:
        return 1 if int(float(x)) != 0 else 0
    except Exception:
        return 0

def _load_store(path: Path, size: int) -> list[int]:
    """
    Carga palabras crudas desde JSON (list o dict). Si no existe o hay error, usa ceros.
    No “fuerza” tipos aquí; el tipado se aplica en las escrituras.
    """
    vals = [0] * size
    if not path.exists():
        return vals
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            for i, v in enumerate(raw[:size]):
                vals[i] = _to_u16(v)
        elif isinstance(raw, dict):
            for k, v in raw.items():
                try:
                    i = int(k)
                except Exception:
                    continue
                if 0 <= i < size:
                    vals[i] = _to_u16(v)
    except Exception as e:
        print(f"[STORE] No pude leer {path}: {e}; uso ceros.")
    return vals

def _save_store_atomic(path: Path, data: list[int]) -> None:
    """Guarda snapshot completo como dict {idx: palabra} de forma atómica."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = {str(i): _to_u16(v) for i, v in enumerate(data)}
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        print(f"[STORE] Error guardando {path}: {e}")

def _pack_f32_to_words(f: float) -> tuple[int, int]:
    raw = struct.pack(">f", float(f)) if BYTEORDER == ">" else struct.pack("<f", float(f))
    hi, lo = struct.unpack(">HH", raw)
    return (lo, hi) if WORD_SWAP else (hi, lo)

def _unpack_f32_from_words(w0: int, w1: int) -> float:
    hi, lo = (w1, w0) if WORD_SWAP else (w0, w1)
    raw = struct.pack(">HH", _to_u16(hi), _to_u16(lo))
    return struct.unpack(">f", raw)[0] if BYTEORDER == ">" else struct.unpack("<f", raw)[0]

# -------- DataBlock tipado y persistente --------
class TypedPersistentHRBlock(ModbusSequentialDataBlock):
    """
    - 200 HR con persistencia en JSON (palabras 16-bit).
    - Escrituras:
        * 'bool' → clamp 0/1
        * 'f32'  → no clamp (acepta palabras crudas); si hay par completo, loguea el REAL.
    - Solo persiste si el valor realmente cambió (evita spam).
    """
    def __init__(self, address: int, values: list[int], store_path: Path):
        super().__init__(address, values)
        self._store_path = store_path
        self._persist_lock = _store_lock
        self._size = len(values)

    def setValues(self, address, values):
        # Aplica tipado por-offset
        new_words = []
        for off, raw in enumerate(values):
            idx = address + off
            t = TYPEMAP.get(idx)
            if t == 'f32':
                # parte de un float: no clampa, solo asegura 16-bit
                new_words.append(_to_u16(raw))
            else:
                # por defecto, booleano 0/1
                new_words.append(_clamp_bool01(raw))

        # Compara con el estado actual para evitar persistencia inútil
        prev = list(self.getValues(address, len(new_words)))
        if prev == new_words:
            # Nada cambió; puedes descomentar para ver intentos repetidos:
            # print(f"[HR WRITE] (sin cambio) addr0={address} <- {new_words}")
            return

        # Aplica al datastore interno
        super().setValues(address, new_words)

        # Log básico
        print(f"[HR WRITE] addr0={address} (4x{address+1:05d}) <- {new_words}")

        # Si el write toca un inicio 'f32' y hay par completo, decodifica y loguea
        touched = set(range(address, address + len(new_words)))
        for idx_start, t in TYPEMAP.items():
            if t == 'f32' and (idx_start in touched or (idx_start + 1) in touched):
                try:
                    w0 = int(self.getValues(idx_start, 1)[0])
                    w1 = int(self.getValues(idx_start + 1, 1)[0])
                    fval = _unpack_f32_from_words(w0, w1)
                    print(f"[F32] 4x{idx_start+1:05d} REAL={fval}")
                except Exception as e:
                    print(f"[F32] Error decodificando idx={idx_start}: {e}")

        # Persistencia: snapshot completo actual
        snapshot = [int(v) for v in self.getValues(0, self._size)]
        with self._persist_lock:
            _save_store_atomic(self._store_path, snapshot)

# -------- Servidor --------
async def main():
    initial_vals = _load_store(STORE_PATH, HR_COUNT)
    hr_block = TypedPersistentHRBlock(0, initial_vals, STORE_PATH)

    slave = ModbusSlaveContext(
        di=ModbusSequentialDataBlock(0, [0]*1),
        co=ModbusSequentialDataBlock(0, [0]*1),
        hr=hr_block,
        ir=ModbusSequentialDataBlock(0, [0]*1),
        zero_mode=True,  # 0-based: 4x00001 == index 0
    )
    context = ModbusServerContext(slaves={UNIT_ID: slave}, single=False)

    print(f"Servidor Modbus TCP (HR mixtos persistentes) en {LISTEN_ADDR[0]}:{LISTEN_ADDR[1]}, Unit={UNIT_ID}")
    print(f"HR inicializados desde: {STORE_PATH.resolve() if STORE_PATH.exists() else '(nuevo)'}")
    await StartAsyncTcpServer(context=context, address=LISTEN_ADDR)

if __name__ == "__main__":
    asyncio.run(main())
