
import subprocess, shlex
cmd = "sudo -n /usr/bin/supervisorctl status plc4xclient"
p = subprocess.run(shlex.split(cmd), capture_output=True, text=True, timeout=10)
print("rc=", p.returncode)
print("STDOUT:\n", repr(p.stdout))
print("STDERR:\n", repr(p.stderr))

