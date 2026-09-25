"""Integración con un servidor HTTP real en 127.0.0.1 (socket + requests reales).

Demuestra el arreglo de §5.9.586 de punta a punta: el servidor manda un
Heartbeat y un Start y se QUEDA CALLADO; el cruce tiene que llegar a la
auditoría mientras la conexión sigue abierta (sin boundary siguiente y sin
completar ningún bloque de 1024 bytes).
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from dahua_ivs.audit import AuditWriter
from dahua_ivs.stream import StreamRunner
from tests.conftest import BOGOTA, make_config


def _start_part(capture: bytes) -> bytes:
    sep = b"--myboundary\r\n"
    part = next(p for p in capture.split(sep)[1:] if b"action=Start" in p)
    assert part.endswith(b"\r\n")
    return sep + part[:-2]  # sin el \r\n final: termina justo en el último byte de Content-Length


@pytest.fixture
def camera_server(capture_0814):
    release = threading.Event()
    served = threading.Event()
    heartbeat = b"--myboundary\r\nContent-Type: text/plain\r\nContent-Length: 9\r\n\r\nHeartbeat\r\n\r\n"
    start = _start_part(capture_0814)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def log_message(self, *args):  # silencio
            pass

        def do_GET(self):
            if "getSerialNo" in self.path:
                body = b"sn=9J00000EXAMPLE0\r\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(200)
            self.send_header("Content-Type", "multipart/x-mixed-replace; boundary=myboundary")
            self.end_headers()
            self.wfile.write(heartbeat + start)
            self.wfile.flush()
            served.set()
            release.wait(10)  # la cámara "calla"

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1], served, release
    release.set()
    server.shutdown()
    server.server_close()


def test_start_llega_a_la_auditoria_sin_esperar_mas_datos(tmp_path, camera_server):
    port, served, release = camera_server
    cfg = make_config(camera_host=f"127.0.0.1:{port}", audit_dir=str(tmp_path), stream_idle_timeout=20)
    audit = AuditWriter(tmp_path, 30)
    runner = StreamRunner(cfg, audit=audit, outbox=None, sender=None, tz=BOGOTA, sleep=lambda s: None)
    t = threading.Thread(target=runner.run_once, daemon=True)
    t.start()

    assert served.wait(5)
    deadline = time.monotonic() + 5
    lines: list[str] = []
    while time.monotonic() < deadline:
        files = list(tmp_path.glob("dahua_ivs_audit_*.jsonl"))
        if files:
            lines = files[0].read_text(encoding="utf-8").splitlines()
            if lines:
                break
        time.sleep(0.05)
    assert not release.is_set()  # la conexión sigue abierta y el servidor sigue callado
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["event_id"] == 10013 and rec["device_serial"] == "9J00000EXAMPLE0"
    assert rec["device_ts"] == "2026-09-23T08:15:33.420-05:00"
    release.set()
    t.join(10)
