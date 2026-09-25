from __future__ import annotations

import json
import logging

from dahua_ivs import ADDON_VERSION
from dahua_ivs.parser import MultipartParser, parse_body
from dahua_ivs.records import (
    CROSSING_FIELDS,
    HEARTBEAT_FIELDS,
    ClockSkewMonitor,
    build_crossing,
    build_heartbeat,
    device_datetime,
    iso_ms,
    serialize,
)
from tests.conftest import BOGOTA, FakeClock


def _starts(raw: bytes):
    bodies = [parse_body(p.body) for p in MultipartParser().feed(raw)]
    return [b for b in bodies if b is not None and b.action == "Start"]


def test_device_ts_hora_local_disfrazada_de_epoch():
    """§5.9.625d: 1790151333 = 08:15:33 en Bogotá (no 13:15:33)."""
    assert iso_ms(device_datetime(1790151333, 340, BOGOTA)) == "2026-09-23T08:15:33.340-05:00"


def test_recorte_exacto_de_campos(capture_0814):
    body = _starts(capture_0814)[0]
    clock = FakeClock()
    rec = build_crossing(
        body, device_serial="9J00000EXAMPLE0", device_ip="192.0.2.21", received_at=clock.now(), tz=BOGOTA
    )
    assert tuple(rec) == CROSSING_FIELDS
    assert rec == {
        "device_kind": "camera_tripwire",
        "kind": "crossing",
        "device_serial": "9J00000EXAMPLE0",
        "device_ip": "192.0.2.21",
        "device_ts": "2026-09-23T08:15:33.420-05:00",
        "received_ts": "2026-09-23T08:15:34.120-05:00",
        "device_utc_raw": {"utc": 1790151333, "utcms": 420},
        "channel": 0,
        "rule_name": "salida 1",  # la regla real tiene espacio final
        "rule_id": 4,
        "event_id": 10013,
        "direction": "LeftToRight",
        "object_id": 73,
        "object_type": "Human",
        "bbox": [2384, 3496, 3712, 8184],
        "center": [3048, 5840],
        "addon_version": ADDON_VERSION,
    }
    assert "raw" not in rec


def test_dos_cruces_en_el_mismo_segundo_son_dos_registros(capture_same_second):
    starts = _starts(capture_same_second)
    recs = [
        build_crossing(b, device_serial="S", device_ip="ip", received_at=FakeClock().now(), tz=BOGOTA) for b in starts
    ]
    mismo_segundo = [r for r in recs if r["device_utc_raw"]["utc"] == 1790156285]
    assert len(mismo_segundo) == 2
    a, b = mismo_segundo
    assert a["device_ts"] != b["device_ts"]  # distintos por los milisegundos
    assert (a["object_id"], b["object_id"]) == (508, 509)
    assert (a["event_id"], b["event_id"]) == (10185, 10187)
    # La llave de idempotencia documentada los distingue.
    key = lambda r: (r["device_serial"], r["rule_id"], r["object_id"], r["device_ts"])  # noqa: E731
    assert key(a) != key(b)


def test_heartbeat_campos():
    rec = build_heartbeat(
        device_serial="S",
        device_ip="ip",
        received_at=FakeClock().now(),
        crossings_since_last=3,
        camera_heartbeats_since_last=30,
        reconnects_since_last=0,
        outbox_pending=None,
    )
    assert tuple(rec) == HEARTBEAT_FIELDS
    assert rec["kind"] == "heartbeat" and rec["device_kind"] == "camera_tripwire"


def test_serializacion_estable():
    rec = {"b": 1, "a": "ñ", "c": [1, 2]}
    s = serialize(rec)
    assert s == '{"b":1,"a":"ñ","c":[1,2]}'
    assert serialize(json.loads(s)) == s


def test_reloj_desfasado_avisa_una_vez_cada_10_min(caplog):
    clock = FakeClock()
    mon = ClockSkewMonitor(monotonic=clock.monotonic)
    dev = "2026-09-23T08:15:33.000-05:00"
    with caplog.at_level(logging.WARNING, logger="dahua_ivs.records"):
        assert mon.check(dev, "2026-09-23T08:17:00.000-05:00") is False  # 87 s: dentro
        assert mon.check(dev, "2026-09-23T13:15:33.000-05:00") is True  # 5 h: fuera
        clock.advance(300)
        assert mon.check(dev, "2026-09-23T13:15:33.000-05:00") is False  # rate limit
        clock.advance(301)
        assert mon.check(dev, "2026-09-23T13:15:33.000-05:00") is True
    assert sum("Reloj desfasado" in r.getMessage() for r in caplog.records) == 2
