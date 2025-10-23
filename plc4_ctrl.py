# --- ajustes de tiempo ---
SUPERVISORCTL = "/usr/bin/supervisorctl"
PROGRAM       = "plc4xclient"
USE_SUDO      = True

CMD_TIMEOUT_S      = 6.0     # antes 3.0
STATUS_POLL_EVERY  = 0.5     # un poco más laxo
STOP_OVERALL_S     = 45.0    # antes 30
START_OVERALL_S    = 45.0    # antes 30

import subprocess, shlex, time, re
from typing import Tuple

def _run(cmd: str, timeout: float = CMD_TIMEOUT_S) -> Tuple[int, str, str]:
    if USE_SUDO:
        cmd = "sudo -n " + cmd
    try:
        proc = subprocess.run(
            shlex.split(cmd),
            capture_output=True, text=True,
            timeout=timeout, check=False
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except subprocess.TimeoutExpired as e:
        # 124 para distinguir timeout de fallo real
        return 124, (e.stdout or "").strip(), (e.stderr or f"timeout after {timeout}s")

_STATE_RE = re.compile(r"\b(RUNNING|STOPPED|STARTING|STOPPING|BACKOFF|FATAL|EXITED)\b", re.I)

import re
_STATE_RE = re.compile(r"\b(RUNNING|STOPPED|STARTING|STOPPING|BACKOFF|FATAL|EXITED)\b", re.I)

def _status(program: str = PROGRAM) -> str:
    """Devuelve RUNNING/STOPPED/... o 'UNKNOWN' si no pudo determinarlo."""
    rc, out, err = _run(f"{SUPERVISORCTL} status {program}")
    # Aceptar rc != 0 si hay salida útil
    text = (out or "") + ("\n" + err if err else "")
    if not text.strip():
        # reintento rápido si no hubo salida
        rc, out, err = _run(f"{SUPERVISORCTL} status {program}")
        text = (out or "") + ("\n" + err if err else "")
        if not text.strip():
            return "UNKNOWN"

    # Si hay varias líneas, busca la que tenga el nombre del programa
    line = ""
    for ln in text.splitlines():
        if ln.strip().startswith(program):
            line = ln
            break
    if not line:
        line = text.splitlines()[0] if text.splitlines() else ""

    m = _STATE_RE.search(line)
    return m.group(1).upper() if m else "UNKNOWN"


def _wait_for(target: str, overall_timeout: float) -> bool:
    target = target.upper()
    t0 = time.time()
    while time.time() - t0 < overall_timeout:
        st = _status()
        if st == target:
            return True
        time.sleep(STATUS_POLL_EVERY)
    return False

def stop_plc4x() -> bool:
    if _status() == "STOPPED":
        return True
    _run(f"{SUPERVISORCTL} stop {PROGRAM}")  # no bloquear aquí
    if not _wait_for("STOPPED", STOP_OVERALL_S):
        # último intento de lectura para log/diagnóstico
        last = _status()
        raise TimeoutError(f"No llegó a STOPPED en {STOP_OVERALL_S}s (estado='{last}')")
    return True

def start_plc4x() -> bool:
    if _status() == "RUNNING":
        return True
    _run(f"{SUPERVISORCTL} start {PROGRAM}")
    if not _wait_for("RUNNING", START_OVERALL_S):
        last = _status()
        raise TimeoutError(f"No llegó a RUNNING en {START_OVERALL_S}s (estado='{last}')")
    return True

def restart_plc4x() -> bool:
    rc, _, _ = _run(f"{SUPERVISORCTL} restart {PROGRAM}")
    if rc == 0 and _wait_for("RUNNING", START_OVERALL_S):
        return True
    # fallback
    stop_plc4x()
    start_plc4x()
    return True
stop_plc4x()
