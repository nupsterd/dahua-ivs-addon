from __future__ import annotations

import logging

import pytest

from dahua_ivs.parser import MultipartParser, boundary_from_content_type, parse_body


def _feed_in_chunks(data: bytes, size: int) -> list:
    parser = MultipartParser("myboundary")
    parts = []
    for i in range(0, len(data), size):
        parts.extend(parser.feed(data[i : i + size]))
    return parts


def _summary(parts) -> dict[str, int]:
    out = {"heartbeat": 0, "Start": 0, "Stop": 0}
    for p in parts:
        body = parse_body(p.body)
        assert body is not None
        if body.heartbeat:
            out["heartbeat"] += 1
        else:
            assert body.code == "CrossLineDetection"
            out[body.action] += 1
    return out


@pytest.mark.parametrize("size", [1, 7, 1024])
def test_replay_captura_real_en_chunks(capture_0814, size):
    parts = _feed_in_chunks(capture_0814, size)
    assert _summary(parts) == {"heartbeat": 50, "Start": 12, "Stop": 11}
    for p in parts:
        assert p.headers["content-type"] == "text/plain"
        assert int(p.headers["content-length"]) == len(p.body)


@pytest.mark.parametrize("size", [1, 7, 1024])
def test_start_se_entrega_antes_del_boundary_siguiente(capture_0814, size):
    """§5.9.586: el Start sale apenas llegan sus Content-Length bytes."""
    start_at = capture_0814.index(b"Code=CrossLineDetection;action=Start")
    # Fin exacto del cuerpo del primer Start (sus bytes de Content-Length).
    header_end = capture_0814.rindex(b"\r\n\r\n", 0, start_at) + 4
    cl_line = capture_0814.rindex(b"Content-Length: ", 0, start_at)
    length = int(capture_0814[cl_line + 16 : capture_0814.index(b"\r\n", cl_line)])
    body_end = header_end + length
    truncated = capture_0814[:body_end]  # sin el \r\n final ni el boundary siguiente
    assert b"--myboundary" not in capture_0814[start_at:body_end]

    parts = _feed_in_chunks(truncated, size)
    last = parse_body(parts[-1].body)
    assert last is not None and last.action == "Start"
    assert last.data is not None and last.data["UTC"] == 1790151333


def test_json_de_data_multilinea(capture_0814):
    parts = _feed_in_chunks(capture_0814, 4096)
    starts = [p for p in parts if b"action=Start" in p.body]
    assert b"data={\n" in starts[0].body
    body = parse_body(starts[0].body)
    assert body.fields == {"Code": "CrossLineDetection", "action": "Start", "index": "0"}
    assert body.data["Name"] == "salida 1 "


def test_fallback_sin_content_length_entrega_al_boundary_siguiente(caplog):
    stream = (
        b"--myboundary\r\nContent-Type: text/plain\r\n\r\nHeartbeat\r\n\r\n"
        b"--myboundary\r\nContent-Type: text/plain\r\n\r\n"
        b'Code=CrossLineDetection;action=Start;index=0;data={\n "UTC" : 1, "UTCMS" : 2\n}\n\r\n'
    )
    parser = MultipartParser()
    with caplog.at_level(logging.WARNING, logger="dahua_ivs.parser"):
        parts = parser.feed(stream)
        # El Start queda retenido hasta el boundary siguiente.
        assert [p.body for p in parts] == [b"Heartbeat\r\n"]
        parts = parser.feed(b"--myboundary\r\n")
    assert len(parts) == 1
    body = parse_body(parts[0].body)
    assert body.action == "Start" and body.data == {"UTC": 1, "UTCMS": 2}
    avisos = [r for r in caplog.records if "sin Content-Length" in r.getMessage()]
    assert len(avisos) == 1  # aviso único


def test_tolera_lf_sin_cr():
    stop = b'Code=CrossLineDetection;action=Stop;index=0;data={"UTC":5}'
    stream = (
        b"--myboundary\nContent-Type: text/plain\nContent-Length: 9\n\nHeartbeat\n\n"
        b"--myboundary\nContent-Length: " + str(len(stop)).encode() + b"\n\n" + stop
    )
    parts = MultipartParser().feed(stream)
    assert parts[0].body == b"Heartbeat"
    assert parse_body(parts[0].body).heartbeat
    assert parse_body(parts[1].body).action == "Stop"


def test_parse_body_tolera_crlf_en_el_json():
    body = parse_body(b'Code=CrossLineDetection;action=Start;index=0;data={\r\n "UTC" : 7\r\n}\r\n')
    assert body.data == {"UTC": 7}


def test_parse_body_json_roto_devuelve_none(caplog):
    assert parse_body(b"Code=CrossLineDetection;action=Start;index=0;data={roto") is None


@pytest.mark.parametrize(
    ("ct", "esperado"),
    [
        ("multipart/x-mixed-replace; boundary=myboundary", "myboundary"),
        ('multipart/x-mixed-replace; boundary="otro"', "otro"),
        ("multipart/x-mixed-replace; boundary=--conguiones", "conguiones"),
        (None, "myboundary"),
        ("text/plain", "myboundary"),
    ],
)
def test_boundary_desde_content_type(ct, esperado):
    assert boundary_from_content_type(ct) == esperado


def test_boundary_propio_del_content_type():
    data = b"--xyz\r\nContent-Length: 9\r\n\r\nHeartbeat\r\n\r\n--xyz\r\n"
    parts = MultipartParser("xyz").feed(data)
    assert [p.body for p in parts] == [b"Heartbeat"]
