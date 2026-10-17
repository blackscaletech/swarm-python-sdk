"""Run all standalone unit tests with loopback-only networking and zero skips."""
from pathlib import Path
import os
import subprocess
import sys
import tempfile


PROGRAM = r'''
from pathlib import Path
import json
import os
import socket
import sys
import unittest
from unittest.mock import patch

root = Path.cwd().resolve()
allowed = (root, Path(sys.base_prefix).resolve(), Path(sys.prefix).resolve())
loopback = {"127.0.0.1", "::1"}

def loopback_fqdn(host=""):
    # HTTPServer asks for a reverse lookup even for numeric loopback fixtures.
    if host not in loopback:
        raise RuntimeError("Unit tests prohibit external DNS")
    return "localhost"

def local_only(event, args):
    if event == "open" and args and isinstance(args[0], (str, bytes)):
        path = Path(os.fsdecode(args[0])).resolve()
        if path != Path(os.devnull) and not any(path.is_relative_to(base) for base in allowed):
            raise RuntimeError("Unit tests prohibit private-source reads: " + str(path))
    if event in ("socket.bind", "socket.connect"):
        address = args[1]
        if not isinstance(address, tuple) or address[0] not in loopback:
            raise RuntimeError("Unit tests permit loopback sockets only")
    elif event == "socket.getaddrinfo":
        if args[0] not in loopback:
            raise RuntimeError("Unit tests prohibit external DNS")
    elif event.startswith("socket.") and event not in ("socket.__new__", "socket.gethostname"):
        raise RuntimeError("Unit tests prohibit external networking")

sys.dont_write_bytecode = True
sys.addaudithook(local_only)
with patch.object(socket, "getfqdn", loopback_fqdn):
    suite = unittest.defaultTestLoader.discover("tests")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
print(json.dumps({"tests": result.testsRun, "failures": len(result.failures),
                  "errors": len(result.errors), "skipped": len(result.skipped),
                  "loopback_only": True, "private_source_reads_denied": True}))
raise SystemExit(0 if result.wasSuccessful() and result.testsRun and not result.skipped else 1)
'''


def main():
    root = Path(__file__).resolve().parents[1]
    receipts = root / ".local-receipts"
    receipts.mkdir(exist_ok=True)
    env = {key: value for key, value in os.environ.items()
           if key not in ("PYTHONPATH", "PYTHONHOME", "PYTHONOPTIMIZE")}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1", NO_PROXY="*")
    with tempfile.TemporaryDirectory(prefix="unit-", dir=receipts) as temporary:
        env.update(TMPDIR=temporary, TMP=temporary, TEMP=temporary)
        return subprocess.run([sys.executable, "-B", "-s", "-c", PROGRAM], cwd=root,
                              env=env, timeout=120).returncode


if __name__ == "__main__":
    raise SystemExit(main())
