from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

import pytest
import requests

from dahua_ivs.audit import AuditWriter
from dahua_ivs.outbox import Outbox, OutboxSender
from dahua_ivs.stream import AUTH_WAIT_SECONDS, Backoff, StreamRunner, Ticker, iter_available, parse_serial
from tests.conftest import BOGOTA, FakeClock, make_config

HB = b"--myboundary\r\nContent-Type: text/plain\r\nContent-Length: 9\r\n\r\nHeartbeat\r\n\r\n"


class _Reader:
    def __init__(self, chunks: list[bytes], exc: BaseException | None) -> None:
        self._chunks = list(chunks)
        self._exc = exc

    def read1(self, _size: int) -> bytes:
        if self._chunks:
            return self._chunks.pop(0)
        if self._exc is not None:
            raise self._exc
        return b""


class _RawUrllib3v1:
    """Como urllib3 1.26: sin read1 propio, pero con _fp.read1 (http.client)."""

    def __init__(self, reader: _Reader) -> None:
        self._fp = reader


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        chunks: list[bytes] | None = None,
        exc: BaseException | None = None,
        text: str = "",
        v1: bool = False,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.headers = {"Content-Type": "multipart/x-mixed-replace; boundary=myboundary"}
        reader = _Reader(chunks or [], exc)
        self.raw = _RawUrllib3v1(reader) if v1 else reader
        self.closed = False

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def close(self) -> None:
        self.closed = True


class FakeCamera:
    """``http_get`` guionado: respuestas (o excepciones) para serial y attach."""

    def __init__(self, serial: list | None = None, attach: list | None = None) -> None:
        self.serial = list(serial or [])
        self.attach = list(attach or [])
        self.calls: list[str] = []

    def get(self, url: str, **kwargs):
        if "getSerialNo" in url:
            self.calls.append("serial")
            item = self.serial.pop(0)
        else:
            self.calls.append("attach")
            assert kwargs["stream"] is True
            item = self.attach.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def serial_ok() -> FakeResponse:
    return FakeResponse(text="sn=9J00000EXAMPLE0\r\n")


def make_runner(tmp_path: Path, camera: FakeCamera, clock: FakeClock, *, outbox: Outbox | None = None, **cfg):
    config = make_config(audit_dir=str(tmp_path / "audit"), **cfg)
    audit = AuditWriter(config.audit_dir, config.audit_retention_days, now=clock.now)
    return StreamRunner(
        config,
        audit=audit,
        outbox=outbox,
        sender=None,
        tz=BOGOTA,
        http_get=camera.get,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        now=clock.now,
    )


def audit_lines(tmp_path: Path) -> list[str]:
    files = sorted((tmp_path / "audit").glob("dahua_ivs_audit_*.jsonl"))
    return [line for f in files for line in f.read_text(encoding="utf-8").splitlines()]


# -- serial ---------------------------------------------------------------


def test_parse_serial():
    assert parse_serial("sn=9J00000EXAMPLE0\r\n") == "9J00000EXAMPLE0"
    assert parse_serial("Error\r\n") is None


def test_sin_serial_no_se_abre_el_stream(tmp_path):
    clock = FakeClock()
    cam = FakeCamera(serial=[requests.ConnectionError("x"), requests.ConnectionError("x"), serial_ok()],
                     attach=[requests.ConnectionError("fin")])
    r = make_runner(tmp_path, cam, clock)
    r.run_once()
    r.run_once()
    assert cam.calls == ["serial", "serial"]
    assert clock.sleeps == [5, 10]
    r.run_once()
    assert cam.calls == ["serial", "serial", "serial", "attach"]
    assert r.serial == "9J00000EXAMPLE0"


@pytest.mark.parametrize("donde", ["serial", "attach"])
def test_401_error_claro_y_espera_15_min(tmp_path, caplog, donde):
    clock = FakeClock()
    if donde == "serial":
        cam = FakeCamera(serial=[FakeResponse(401)])
    else:
        cam = FakeCamera(serial=[serial_ok()], attach=[FakeResponse(401)])
    r = make_runner(tmp_path, cam, clock)
    with caplog.at_level(logging.ERROR):
        r.run_once()
    assert clock.sleeps == [AUTH_WAIT_SECONDS] == [900]
    assert any("401" in rec.getMessage() and "camera_password" in rec.getMessage() for rec in caplog.records)


# -- backoff --------------------------------------------------------------


def test_backoff_secuencia_y_tope():
    b = Backoff()
    assert [b.next() for _ in range(7)] == [5, 10, 20, 40, 60, 60, 60]
    b.reset()
    assert b.next() == 5


def test_backoff_vuelve_a_5_solo_con_el_primer_heartbeat(tmp_path, capture_same_second):
    clock = FakeClock()
    sep = b"--myboundary\r\n"
    start_only = b"".join(sep + p for p in capture_same_second.split(sep)[1:] if b"Heartbeat" not in p)
    assert b"Heartbeat" not in start_only and start_only.count(b"action=Start") == 3
    cam = FakeCamera(
        serial=[serial_ok()],
        attach=[
            requests.ConnectionError("1"),
            requests.ConnectionError("2"),
            requests.ConnectionError("3"),
            requests.ConnectionError("4"),
            requests.ConnectionError("5"),
            FakeResponse(chunks=[start_only]),  # conecta, cruces pero SIN Heartbeat
            FakeResponse(chunks=[HB], exc=TimeoutError("timed out")),  # Heartbeat y luego idle
            requests.ConnectionError("6"),
        ],
    )
    r = make_runner(tmp_path, cam, clock)
    for _ in range(8):
        r.run_once()
    # 5 fallos, conexión sin Heartbeat (no resetea: 60), conexión con Heartbeat (reset: 5), fallo (10).
    assert clock.sleeps == [5, 10, 20, 40, 60, 60, 5, 10]
    assert r.counters.reconnects == 7


def test_stream_real_por_trozos_audita_y_encola_lo_mismo(tmp_path, capture_same_second):
    clock = FakeClock()
    chunks = [capture_same_second[i : i + 7] for i in range(0, len(capture_same_second), 7)]
    ob = Outbox(tmp_path / "outbox.sqlite", 1000, 7, clock=clock.time, monotonic=clock.monotonic)
    cam = FakeCamera(serial=[serial_ok()], attach=[FakeResponse(chunks=chunks, v1=True)])
    r = make_runner(tmp_path, cam, clock, outbox=ob)
    r.run_once()
    lines = audit_lines(tmp_path)
    assert len(lines) == 3  # 3 Start; Stop y Heartbeat no generan registro
    assert [json.loads(line)["event_id"] for line in lines] == [10185, 10187, 10193]
    queued = []
    while (head := ob.peek()) is not None:
        queued.append(head[1])
        ob.delete(head[0])
    assert queued == lines  # byte a byte la misma cadena
    assert r.counters.crossings == 3 and r.counters.camera_heartbeats == 2


def test_iter_available_no_espera_a_llenar_el_bloque():
    resp = FakeResponse(chunks=[b"abc", b"d"])
    assert list(iter_available(resp)) == [b"abc", b"d"]
    resp_v1 = FakeResponse(chunks=[b"xy"], v1=True)
    assert list(iter_available(resp_v1)) == [b"xy"]


def test_url_attach():
    r = make_runner(Path("/tmp"), FakeCamera(), FakeClock(), camera_heartbeat_seconds=30)
    assert r.attach_url == (
        "http://192.0.2.21/cgi-bin/eventManager.cgi?action=attach&codes=%5BCrossLineDetection%5D&heartbeat=30"
    )


# -- latido propio --------------------------------------------------------


def test_latido_solo_con_stream_sano(tmp_path):
    clock = FakeClock()
    r = make_runner(tmp_path, FakeCamera(), clock, heartbeat_interval_minutes=15, stream_idle_timeout=75)
    r.serial = "S"
    t = Ticker(r, monotonic=clock.monotonic, now=clock.now)

    clock.advance(15 * 60)
    t.tick()
    assert audit_lines(tmp_path) == []  # nunca llegó un Heartbeat de la cámara

    r.handle_part_body(b"Heartbeat")
    clock.advance(80)  # último Heartbeat hace 80 s > idle 75 ⇒ no sano
    t.tick()
    assert audit_lines(tmp_path) == []

    r.handle_part_body(b"Heartbeat")  # vuelve a estar sano ⇒ sale el latido pendiente
    t.tick()
    lines = [json.loads(x) for x in audit_lines(tmp_path)]
    assert len(lines) == 1
    hb = lines[0]
    assert hb["kind"] == "heartbeat" and hb["camera_heartbeats_since_last"] == 2
    assert hb["outbox_pending"] is None and hb["device_serial"] == "S"

    clock.advance(60)
    r.handle_part_body(b"Heartbeat")
    t.tick()  # todavía no se cumplió el intervalo
    assert len(audit_lines(tmp_path)) == 1


def test_resumen_horario_info(tmp_path, caplog):
    clock = FakeClock()
    r = make_runner(tmp_path, FakeCamera(), clock)
    t = Ticker(r, monotonic=clock.monotonic, now=clock.now)
    r.handle_part_body(b"Heartbeat")
    clock.advance(3600)
    with caplog.at_level(logging.INFO, logger="dahua_ivs.stream"):
        t.tick()
    assert any("Resumen última hora" in rec.getMessage() and "latidos_camara=1" in rec.getMessage()
               for rec in caplog.records)


def _resumenes(caplog) -> list[str]:
    return [rec.getMessage() for rec in caplog.records if "Resumen última hora" in rec.getMessage()]


class _PostBloqueable:
    """``post`` del sender: responde ``received`` al instante, salvo que se cierre la
    compuerta; entonces el POST queda EN VUELO hasta que el test la abra."""

    def __init__(self) -> None:
        self.abierta = threading.Event()
        self.abierta.set()
        self.en_vuelo = threading.Event()

    def __call__(self, _url, _data, _headers, _timeout):
        if not self.abierta.is_set():
            self.en_vuelo.set()
            self.abierta.wait(10)

        class _Resp:
            status_code = 200
            text = '{"status": "received"}'

        return _Resp()


def _esperar(cond, timeout: float = 5.0) -> None:
    fin = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < fin, "timeout esperando al hilo de envío"
        time.sleep(0.01)


def test_resumen_no_cuenta_el_latido_en_vuelo(tmp_path, caplog):
    """#28: el latido de las :00 y el resumen vencen en la misma vuelta. Con el latido
    encolado y su POST en vuelo, el resumen tiene que decir pendientes_cola=0."""
    clock = FakeClock()
    outbox = Outbox(tmp_path / "outbox.sqlite", 1000, 7, clock=clock.time)
    r = make_runner(tmp_path, FakeCamera(), clock, outbox=outbox, heartbeat_interval_minutes=15)
    r.serial = "S"
    post = _PostBloqueable()
    r.sender = OutboxSender(outbox, "http://backend/api", "tok", 5, post=post)
    stop = threading.Event()
    hilo = threading.Thread(target=r.sender.run, args=(stop,), daemon=True)
    hilo.start()
    t = Ticker(r, monotonic=clock.monotonic, now=clock.now)
    try:
        for _ in range(3):  # latidos de :15, :30 y :45, que drenan normalmente
            clock.advance(15 * 60)
            r.handle_part_body(b"Heartbeat")
            t.tick()
            _esperar(lambda: outbox.pending_count() == 0)

        post.abierta.clear()  # el próximo POST queda en vuelo
        clock.advance(15 * 60)
        r.handle_part_body(b"Heartbeat")
        with caplog.at_level(logging.INFO, logger="dahua_ivs.stream"):
            t.tick()  # latido de :00 + resumen en la MISMA vuelta
        post.en_vuelo.wait(5)
        assert post.en_vuelo.is_set() and outbox.pending_count() == 1  # el latido, en vuelo
        assert _resumenes(caplog) == [
            "Resumen última hora: cruces=0 latidos_camara=4 reconexiones=0 pendientes_cola=0"
        ]
    finally:
        post.abierta.set()
        stop.set()
        r.sender.notify()
        hilo.join(5)
    _esperar(lambda: outbox.pending_count() == 0)
    latidos = [json.loads(x) for x in audit_lines(tmp_path)]
    assert [hb["outbox_pending"] for hb in latidos] == [0, 0, 0, 0]


def test_resumen_cuenta_un_registro_atascado(tmp_path, caplog):
    """Un registro que NO drena (backend caído o pausa por configuración: nadie lo
    borra) sigue en el resumen, junto con lo que quedó detrás de él en la cola FIFO."""
    clock = FakeClock()
    outbox = Outbox(tmp_path / "outbox.sqlite", 1000, 7, clock=clock.time)
    r = make_runner(tmp_path, FakeCamera(), clock, outbox=outbox, heartbeat_interval_minutes=15)
    r.serial = "S"
    t = Ticker(r, monotonic=clock.monotonic, now=clock.now)
    r.emit('{"kind": "crossing", "atascado": true}')  # la cabeza que no drena
    for _ in range(4):
        clock.advance(15 * 60)
        r.handle_part_body(b"Heartbeat")
        with caplog.at_level(logging.INFO, logger="dahua_ivs.stream"):
            t.tick()
    # En la foto de las :00: el cruce atascado + los latidos de :15, :30 y :45 (no el de :00).
    assert _resumenes(caplog) == [
        "Resumen última hora: cruces=0 latidos_camara=4 reconexiones=0 pendientes_cola=4"
    ]
    assert outbox.pending_count() == 5
    latidos = [json.loads(x) for x in audit_lines(tmp_path) if '"heartbeat"' in x]
    assert [hb["outbox_pending"] for hb in latidos] == [1, 2, 3, 4]


def test_resumen_solo_con_atascado_de_una_hora_atras(tmp_path, caplog):
    """Mínimo del requisito duro: un único registro atascado ⇒ pendientes_cola >= 1."""
    clock = FakeClock()
    outbox = Outbox(tmp_path / "outbox.sqlite", 1000, 7, clock=clock.time)
    r = make_runner(tmp_path, FakeCamera(), clock, outbox=outbox)
    t = Ticker(r, monotonic=clock.monotonic, now=clock.now)
    r.emit('{"kind": "crossing", "atascado": true}')
    clock.advance(3600)  # stream no sano: no hay latidos, solo el resumen
    with caplog.at_level(logging.INFO, logger="dahua_ivs.stream"):
        t.tick()
    assert _resumenes(caplog) == [
        "Resumen última hora: cruces=0 latidos_camara=0 reconexiones=0 pendientes_cola=1"
    ]


def test_tick_sin_nada_vencido_no_lee_la_cola(tmp_path):
    clock = FakeClock()
    outbox = Outbox(tmp_path / "outbox.sqlite", 1000, 7, clock=clock.time)
    r = make_runner(tmp_path, FakeCamera(), clock, outbox=outbox)
    t = Ticker(r, monotonic=clock.monotonic, now=clock.now)
    outbox.close()  # si tick() tocara la cola, fallaría
    clock.advance(60)
    t.tick()


def test_cruce_no_se_imprime_a_nivel_info(tmp_path, capture_same_second, caplog):
    clock = FakeClock()
    cam = FakeCamera(serial=[serial_ok()], attach=[FakeResponse(chunks=[capture_same_second])])
    r = make_runner(tmp_path, cam, clock)
    with caplog.at_level(logging.INFO):
        r.run_once()
    assert not any("10185" in rec.getMessage() for rec in caplog.records)
