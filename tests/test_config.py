from __future__ import annotations

import json
import logging
from dataclasses import fields
from pathlib import Path

import pytest
import requests
import yaml

from dahua_ivs import ADDON_VERSION
from dahua_ivs.config import Config
from dahua_ivs.main import build, startup
from dahua_ivs.outbox import Outbox, OutboxSender
from tests.conftest import BACKEND_SECRET, CAMERA_PASSWORD, FakeClock, make_config

ROOT = Path(__file__).resolve().parent.parent


def _yaml() -> dict:
    return yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))


def test_config_yaml_tiene_las_mismas_opciones_que_config():
    y = _yaml()
    nombres = [f.name for f in fields(Config)]
    assert list(y["options"]) == nombres
    assert list(y["schema"]) == nombres
    defaults = Config.from_dict(y["options"])
    for f in fields(Config):
        if f.name == "camera_host":
            continue  # obligatoria, sin default en la clase
        assert getattr(defaults, f.name) == f.default, f.name


def test_config_yaml_metadatos():
    y = _yaml()
    assert y["slug"] == "dahua_ivs"
    assert y["name"] == "Dahua IVS (Portería Virtual)"
    assert y["version"] == ADDON_VERSION == "0.1.1-alpha"
    assert y["boot"] == "auto" and y["startup"] == "services" and y["init"] is False
    assert "image" not in y  # build local en la Pi
    assert y["map"] == ["addon_config:rw"]
    assert y["schema"]["log_level"] == "list(debug|info|warning)"
    assert y["schema"]["camera_password"] == "password"
    assert y["schema"]["backend_secret"] == "password?"


def test_from_options_json(tmp_path):
    p = tmp_path / "options.json"
    p.write_text(json.dumps({**_yaml()["options"], "camera_host": "192.0.2.21", "stream_idle_timeout": "90"}))
    cfg = Config.from_options_json(str(p))
    assert cfg.camera_host == "192.0.2.21" and cfg.stream_idle_timeout == 90
    assert cfg.backend_enabled is False


@pytest.mark.parametrize(("hb", "idle", "ok"), [(30, 60, True), (30, 75, True), (30, 59, False), (60, 100, False)])
def test_validacion_idle_mayor_o_igual_a_2x_heartbeat(hb, idle, ok):
    errores = make_config(camera_heartbeat_seconds=hb, stream_idle_timeout=idle).validate()
    assert (errores == []) is ok


def test_arranque_con_config_invalida_sale_con_error(caplog):
    cfg = make_config(camera_heartbeat_seconds=60, stream_idle_timeout=75)
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as ei:
        startup(cfg)
    assert ei.value.code == 1
    assert any("stream_idle_timeout" in r.getMessage() for r in caplog.records)


def test_zona_invalida():
    assert make_config(camera_timezone="Marte/Olympus").validate()


def test_secretos_nunca_en_el_log(tmp_path, caplog):
    """camera_password y backend_secret no aparecen en ningún log (solo configurado/vacío)."""
    from tests.test_stream import FakeCamera, FakeResponse, make_runner, serial_ok

    cfg = make_config(
        backend_url="https://api.example.test/api/v1/eventos/dahua",
        backend_secret=BACKEND_SECRET,
        audit_dir=str(tmp_path / "audit"),
        log_level="debug",
    )
    assert CAMERA_PASSWORD not in repr(cfg) and BACKEND_SECRET not in repr(cfg)
    with caplog.at_level(logging.DEBUG):
        startup(cfg)
        build(cfg, outbox_path=str(tmp_path / "outbox.sqlite"))
        # 401 de la cámara y del backend, y un stream con cruces en DEBUG.
        clock = FakeClock()
        make_runner(tmp_path, FakeCamera(serial=[FakeResponse(401)]), clock).run_once()
        hb = b"--myboundary\r\nContent-Length: 9\r\n\r\nHeartbeat"
        cam = FakeCamera(serial=[serial_ok()], attach=[FakeResponse(chunks=[hb])])
        make_runner(tmp_path, cam, clock).run_once()
        ob = Outbox(tmp_path / "o2.sqlite", 1000, 7)
        ob.put("{}")

        class R401:
            status_code = 401
            text = '{"detail":"token invalido"}'

        OutboxSender(ob, cfg.backend_url, BACKEND_SECRET, 5, post=lambda *a: R401()).process_one()
        OutboxSender(ob, cfg.backend_url, BACKEND_SECRET, 5,
                     post=lambda *a: (_ for _ in ()).throw(requests.ConnectionError("x"))).process_one()
    texto = caplog.text
    assert "camera_password = configurado" in texto
    assert "backend_secret = configurado" in texto
    assert CAMERA_PASSWORD not in texto
    assert BACKEND_SECRET not in texto
