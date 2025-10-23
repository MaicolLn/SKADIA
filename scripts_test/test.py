from pymodbus.client import ModbusTcpClient
c = ModbusTcpClient("192.168.1.200", port=502, timeout=5)
UNIT = 205  # prueba también 1, 2, 10, 0, 255
rr = c.read_coils(address=0, count=1, slave=UNIT)
