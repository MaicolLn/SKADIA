# diag_corriente.py
from pymodbus.client import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

HOST, PORT, SLAVE = "192.168.1.200", 502, 105
REG_1B = 3060
ADDR0  = REG_1B - 1

def f32(words, wordorder):
    dec = BinaryPayloadDecoder.fromRegisters(words, byteorder=Endian.BIG, wordorder=wordorder)
    return dec.decode_32bit_float()

def main():
    c = ModbusTcpClient(HOST, port=PORT, timeout=3.0, strict=False)
    assert c.connect(), "No conecta"

    try:
        rr3 = c.read_holding_registers(address=ADDR0, count=2, slave=SLAVE)  # FC3
        rr4 = c.read_input_registers(  address=ADDR0, count=2, slave=SLAVE)  # FC4

        print("== FC3 (Holding) ==")
        if rr3 and not rr3.isError():
            w = rr3.registers
            print("raw:", w)
            print("float32 BIG/BIG   :", f32(w, Endian.BIG))
            print("float32 BIG/LITTLE:", f32(w, Endian.LITTLE))
        else:
            print("FC3 error:", rr3)

        print("\n== FC4 (Input) ==")
        if rr4 and not rr4.isError():
            w = rr4.registers
            print("raw:", w)
            print("float32 BIG/BIG   :", f32(w, Endian.BIG))
            print("float32 BIG/LITTLE:", f32(w, Endian.LITTLE))
        else:
            print("FC4 error:", rr4)
    finally:
        c.close()

if __name__ == "__main__":
    main()
