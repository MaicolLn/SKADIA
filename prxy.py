#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pip install "pymodbus>=3.6"

import argparse, socket, time
from typing import List
from pymodbus.client import ModbusTcpClient

def tcp_check(host: str, port: int, timeout: float) -> bool:
    try:
        t0 = time.perf_counter()
        with socket.create_connection((host, port), timeout=timeout):
            dt = (time.perf_counter() - t0) * 1000
            print(f"[TCP] {host}:{port} abierto ({dt:.1f} ms).")
            return True
    except Exception as e:
        print(f"[TCP] {host}:{port} no accesible: {e}")
        return False

def try_fc3(client: ModbusTcpClient, unit: int, addr: int, count: int, timeout: float):
    t0 = time.perf_counter()
    rr = client.read_holding_registers(address=addr, count=count, unit=unit)
    dt = (time.perf_counter() - t0) * 1000
    if rr and not rr.isError():
        print(f"  [OK] FC3 unit={unit} addr={addr} count={count}  {dt:.1f} ms  regs={rr.registers}")
        return True
    print(f"  [--] FC3 unit={unit} addr={addr} -> {getattr(rr, 'exception_code', 'error')}")
    return False

def try_fc1(client: ModbusTcpClient, unit: int, addr: int, count: int, timeout: float):
    t0 = time.perf_counter()
    rr = client.read_coils(address=addr, count=count, unit=unit)
    dt = (time.perf_counter() - t0) * 1000
    if rr and not rr.isError():
        print(f"  [OK] FC1 unit={unit} addr={addr} count={count}  {dt:.1f} ms  bits={rr.bits[:count]}")
        return True
    print(f"  [--] FC1 unit={unit} addr={addr} -> {getattr(rr, 'exception_code', 'error')}")
    return False

def parse_units(s: str) -> List[int]:
    # acepta "205,206" o "1-10"
    units = set()
    for part in s.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            units.update(range(int(a), int(b) + 1))
        else:
            units.add(int(part))
    return sorted(units)

def main():
    ap = argparse.ArgumentParser(description="Sonda Modbus/TCP (descubre units y prueba FC3/FC1).")
    ap.add_argument("--host", required=True, help="IP del gateway (ej. 192.168.1.200)")
    ap.add_argument("--port", type=int, default=502, help="Puerto TCP (por defecto 502)")
    ap.add_argument("--units", default="1-10", help="Lista/rango de Unit IDs. Ej: '205' o '205,206' o '1-10'")
    ap.add_argument("--timeout", type=float, default=3.0, help="Timeout TCP/Modbus (s)")
    ap.add_argument("--addr", type=int, default=0, help="Dirección base a leer (por defecto 0)")
    args = ap.parse_args()

    units = parse_units(args.units)

    if not tcp_check(args.host, args.port, args.timeout):
        return

    client = ModbusTcpClient(args.host, port=args.port, timeout=args.timeout, retries=0, retry_on_empty=True)
    if not client.connect():
        print("[ERR] No se pudo abrir sesión Modbus (connect() falló).")
        return

    try:
        for unit in units:
            print(f"\n== Probing unit {unit} ==")
            ok3 = try_fc3(client, unit, addr=args.addr, count=1, timeout=args.timeout)
            ok1 = try_fc1(client, unit, addr=0, count=1, timeout=args.timeout)
            if not (ok3 or ok1):
                print("  (sin respuesta válida en FC3/FC1; puede ser Unit ID incorrecto o mapa distinto)")
    finally:
        client.close()
        print("\n[FIN]")

if __name__ == "__main__":
    main()
