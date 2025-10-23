from pymodbus.client import ModbusTcpClient
from time import sleep

IP, PORT, UNIT = "192.168.1.200", 502, 205
DEFAULT_DELAY = 1.5  # segundos

def with_client(fn):
    c = ModbusTcpClient(IP, port=PORT, timeout=3, strict=False)
    if not c.connect():
        raise RuntimeError("No conecta")
    try:
        return fn(c)
    finally:
        c.close()

def set_coil(dir_1based: int, value: bool, delay: float = DEFAULT_DELAY):
    """Escribe una coil y espera 'delay' segundos tras la operación."""
    addr = dir_1based - 1
    res = with_client(lambda c: c.write_coil(address=addr, value=value, slave=UNIT))
    sleep(delay)
    return res

def get_coil(dir_1based: int, delay: float = DEFAULT_DELAY):
    """Lee una coil tras esperar 'delay' segundos (útil si acabas de escribir)."""
    addr = dir_1based - 1
    sleep(delay)
    rr = with_client(lambda c: c.read_coils(address=addr, count=1, slave=UNIT))
    return (rr.bits[0] if rr and not rr.isError() else rr)

# Ejemplos de uso:
if __name__ == "__main__":
    # delay de 0.5s entre pasos
    d = 0.5

    print("3 ON :", set_coil(3, True, delay=d))
    print("4 ON :", set_coil(4, True, delay=d))
    sleep(5)
    print("3    :", get_coil(3, delay=d))
    print("4    :", get_coil(4, delay=d))
    sleep(5)
    print("3 OFF:", set_coil(3, False, delay=d))
    print("4 OFF:", set_coil(4, False, delay=d))
    sleep(5)
    print("3    :", get_coil(3, delay=d))
    print("4    :", get_coil(4, delay=d))
