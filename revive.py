# pegalo donde tengas tus helpers (mismo módulo donde está leer_medidor)
import json
from typing import List, Tuple, Optional
from tool_f.modbus_tools import leer_medidor, RUTA_JSON, IP, PORT
# si leer_medidor está en tool_f.modbus_tools, importa desde allí:
# from tool_f.modbus_tools import leer_medidor, RUTA_JSON, IP, PORT

READ_ADDR_1B = 3036  # HR 3036 (1-based)
DEFAULT_PARAM_NAME = "VLN_AVG"  # nombre típico en tu JSON para 3036

def _load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _find_param_name_by_addr(cfg: dict, addr_1b: int) -> Optional[str]:
    """Busca en Registros_Medidor un parámetro cuyo addr == addr_1b."""
    regmap = cfg.get("Registros_Medidor") or {}
    for pname, entry in regmap.items():
        if isinstance(entry, dict):
            if int(entry.get("addr", -1)) == addr_1b:
                return pname
        else:
            # entrada simple: "ParamX": 3036
            if int(entry) == addr_1b:
                return pname
    return None

def ping_medidores_hr3036(
    *,
    ruta_json: str = RUTA_JSON,
    host: str = IP,
    port: int = PORT,
    timeout: float = 2.0,
    prefer_param: str = DEFAULT_PARAM_NAME,
) -> List[Tuple[str, bool, str]]:
    """
    Itera cfg['Medidores'] y lee HR 3036 usando tu leer_medidor.
    - Intenta con 'prefer_param' (p.ej. 'VLN_AVG').
    - Si no existe, autodetecta el nombre cuyo addr==3036.
    Devuelve [(medidor, ok, detalle)] y también imprime línea por línea.
    """
    cfg = _load_cfg(ruta_json)
    medidores = list((cfg.get("Medidores") or {}).keys())
    if not medidores:
        print("[PING] No hay 'Medidores' en el JSON.")
        return []

    # Resolver nombre de parámetro (si preferido no existe)
    param_name = prefer_param
    if param_name not in (cfg.get("Registros_Medidor") or {}):
        auto = _find_param_name_by_addr(cfg, READ_ADDR_1B)
        if auto:
            param_name = auto
        else:
            print(f"[PING] No encontré parámetro con addr {READ_ADDR_1B} ni '{prefer_param}'.")
            return [(m, False, f"sin parámetro {READ_ADDR_1B}") for m in medidores]

    results: List[Tuple[str, bool, str]] = []

    for med in medidores:
        try:
            # Usa tu leer_medidor (respeta decode float32 por defecto)
            val = leer_medidor(
                medidor=med,
                parametro=param_name,
                ruta_json=ruta_json,
                host=host,
                port=port,
                timeout=timeout,
                decode="float32",           # fuerza float32 por si no está en JSON
                return_unit=False
            )
            print(f"OK   {med:<12} [{param_name}@{READ_ADDR_1B}] -> {val}")
            results.append((med, True, f"value={val}"))
        except Exception as e:
            msg = str(e)
            print(f"FAIL {med:<12} [{param_name}@{READ_ADDR_1B}] -> {msg}")
            results.append((med, False, msg))
    return results

# --- Ejemplo de uso ---
if __name__ == "__main__":
    ping_medidores_hr3036(
        ruta_json="mapaIEEE13.json",  # o deja RUTA_JSON
        host="192.168.1.200",
        port=502,
        timeout=2.0,
        prefer_param="VLN_AVG",       # si no existe, auto-descubre por addr=3036
    )
