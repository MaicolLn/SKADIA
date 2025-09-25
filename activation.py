from tool_f.modbus_tools import escribir_plc, leer_plc, leer_medidor

# print(leer_medidor("Medidor_132", "Voltaje_B", return_unit=True))
print(escribir_plc("Nodo_645", "linea_645_646", False))
print(leer_plc("Nodo_645", "carga_1"))
