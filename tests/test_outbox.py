from __future__ import annotations

import json

import pytest
import requests

from dahua_ivs.outbox import AUTH_HEADER, CONFIG_ERROR_DELAY, RETRY_DELAYS, Action, Outbox, OutboxSender, classify
from tests.conftest import BACKEND_SECRET, FakeClock

URL = "https://api.example.test/api/v1/eventos/dahua"


class Resp:
    def __init__(self, status_code: int, body: object = None) -> None:
        self.status_code = status_code
        self.text = body if isinstance(body, str) else json.dumps(body) if body is not None else ""


class FakeBackend:
    """``post`` guionado que registra exactamente los bytes y headers enviados."""

    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.sent: list[tuple[bytes, dict[str, str]]] = []

    def post(self, url: str, data: bytes, headers: dict[str, str], timeout: float):
        assert url == URL
        self.sent.append((data, dict(headers)))
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def make_outbox(tmp_path, clock, **kw) -> Outbox:
    return Outbox(
        tmp_path / "outbox.sqlite",
        kw.get("max_records", 1000),
        kw.get("max_age_days", 7),
        clock=clock.time,
        monotonic=clock.monotonic,
    )


def make_sender(ob: Outbox, backend: FakeBackend) -> OutboxSender:
    return OutboxSender(ob, URL, BACKEND_SECRET, 5, post=backend.post)


def test_persiste_al_reabrir(tmp_path):
    clock = FakeClock()
    ob = make_outbox(tmp_path, clock)
    ob.put('{"n":1}')
    ob.put('{"n":2}')
    ob.close()
    ob2 = make_outbox(tmp_path, clock)
    assert ob2.pending_count() == 2
    assert ob2.peek()[1] == '{"n":1}'
    assert ob2._conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_descarta_el_mas_viejo_por_cantidad(tmp_path, caplog):
    clock = FakeClock()
    ob = make_outbox(tmp_path, clock, max_records=3)
    for n in range(5):
        ob.put(f'{{"n":{n}}}')
    assert ob.pending_count() == 3
    assert ob.peek()[1] == '{"n":2}'
    avisos = [r for r in caplog.records if "MÁS VIEJOS" in r.getMessage()]
    assert len(avisos) == 1  # el segundo descarte cae dentro del límite de frecuencia


def test_descarta_el_mas_viejo_por_edad(tmp_path):
    clock = FakeClock()
    ob = make_outbox(tmp_path, clock, max_age_days=7)
    ob.put('{"viejo":1}')
    clock.advance(6 * 86400)
    ob.put('{"nuevo":1}')
    clock.advance(1 * 86400 + 1)
    assert ob.enforce_limits() == 1
    assert ob.peek()[1] == '{"nuevo":1}'


@pytest.mark.parametrize("status", ["received", "duplicate"])
def test_2xx_received_o_duplicate_borra(tmp_path, status):
    clock = FakeClock()
    ob = make_outbox(tmp_path, clock)
    ob.put('{"a":1}')
    be = FakeBackend([Resp(200, {"status": status})])
    assert make_sender(ob, be).process_one() == 0.0
    assert ob.pending_count() == 0
    data, headers = be.sent[0]
    assert data == b'{"a":1}'
    assert headers[AUTH_HEADER] == BACKEND_SECRET
    assert headers["Content-Type"] == "application/json"


@pytest.mark.parametrize(
    "resp",
    [Resp(200, {"status": "device_unknown"}), Resp(401, {"detail": "token invalido"}), Resp(404, "Not Found")],
    ids=["device_unknown", "401", "404"],
)
def test_error_de_configuracion_pausa_sin_perder_nada(tmp_path, caplog, resp):
    clock = FakeClock()
    ob = make_outbox(tmp_path, clock)
    for n in range(3):
        ob.put(f'{{"n":{n}}}')
    be = FakeBackend([resp, resp, Resp(200, {"status": "received"})])
    sender = make_sender(ob, be)
    assert sender.process_one() == CONFIG_ERROR_DELAY == 600
    assert sender.process_one() == 600  # sigue pausado, reintenta el MISMO registro
    assert ob.pending_count() == 3
    assert sender.paused_reason is not None
    assert [d for d, _ in be.sent] == [b'{"n":0}', b'{"n":0}']
    assert any(r.levelname == "ERROR" and "CONFIGURACIÓN" in r.getMessage() for r in caplog.records)
    # Corregida la configuración, reanuda en orden.
    assert sender.process_one() == 0.0
    assert ob.peek()[1] == '{"n":1}'
    assert sender.paused_reason is None


@pytest.mark.parametrize("code", [400, 422])
def test_400_422_va_a_failed_y_sigue(tmp_path, code):
    clock = FakeClock()
    ob = make_outbox(tmp_path, clock)
    ob.put('{"malo":1}')
    ob.put('{"bueno":1}')
    be = FakeBackend([Resp(code, {"detail": "payload invalido"}), Resp(200, {"status": "received"})])
    sender = make_sender(ob, be)
    assert sender.process_one() == 0.0
    assert sender.process_one() == 0.0
    assert ob.pending_count() == 0
    [(_, status_code, reason, payload)] = ob.failed_rows()
    assert (status_code, payload) == (code, '{"malo":1}')
    assert "payload invalido" in reason


def test_5xx_y_red_reintentan_con_backoff_en_orden_y_byte_a_byte(tmp_path):
    clock = FakeClock()
    ob = make_outbox(tmp_path, clock)
    payload = '{"device_ts":"2026-09-23T08:15:33.420-05:00","rule_name":"salida 1","ñ":"á"}'
    ob.put(payload)
    ob.put('{"segundo":1}')
    script = [Resp(503), requests.ConnectionError("red"), requests.Timeout("t")] + [Resp(500)] * 5
    script += [Resp(200, {"status": "duplicate"})]
    be = FakeBackend(script)
    sender = make_sender(ob, be)
    delays = [sender.process_one() for _ in range(9)]
    assert delays == [5, 10, 20, 40, 80, 160, 300, 300, 0.0]
    assert max(RETRY_DELAYS) == 300
    # Siempre el mismo registro (orden conservado) y exactamente los mismos bytes.
    assert {d for d, _ in be.sent} == {payload.encode("utf-8")}
    assert ob.peek()[1] == '{"segundo":1}'


def test_cola_vacia_devuelve_none(tmp_path):
    ob = make_outbox(tmp_path, FakeClock())
    assert make_sender(ob, FakeBackend([])).process_one() is None


@pytest.mark.parametrize(
    ("code", "body", "action"),
    [
        (200, '{"status":"received","evento_id":1}', Action.DELETE),
        (200, '{"status":"handler_disabled"}', Action.CONFIG_ERROR),
        (200, "no json", Action.CONFIG_ERROR),
        (403, "", Action.CONFIG_ERROR),
        (405, "", Action.CONFIG_ERROR),
        (408, "", Action.RETRY),
        (429, "", Action.RETRY),
        (502, "", Action.RETRY),
        (413, "", Action.FAIL_RECORD),
    ],
)
def test_clasificacion(code, body, action):
    assert classify(code, body).action is action
