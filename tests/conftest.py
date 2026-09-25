"""Fixtures compartidas. Las capturas .raw son reales (23-sep, oficina), sin datos personales:
el payload del tripwire no trae imagen, rostro ni biometría (§5.9.625)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from dahua_ivs.config import Config

FIXTURES = Path(__file__).parent / "fixtures"
BOGOTA = ZoneInfo("America/Bogota")

CAMERA_PASSWORD = "Cam-Secret-no-loguear-7731"
BACKEND_SECRET = "Backend-Token-no-loguear-9920"


@pytest.fixture
def capture_0814() -> bytes:
    """Captura completa 08:14: 50 Heartbeat + 12 Start + 11 Stop, todas con Content-Length."""
    return (FIXTURES / "attach_0923_0814.raw").read_bytes()


@pytest.fixture
def capture_same_second() -> bytes:
    """Extracto 09:31 (sin líneas #reconnect): HB, Start 10185, Start 10187 (mismo segundo),
    Stop 10185, HB, Start 10193."""
    return (FIXTURES / "attach_0923_same_second.raw").read_bytes()


def make_config(**overrides: object) -> Config:
    base: dict[str, object] = {
        "camera_host": "192.0.2.21",
        "camera_password": CAMERA_PASSWORD,
        "audit_dir": "/tmp/no-usado",
    }
    base.update(overrides)
    return Config.from_dict(base)


class FakeClock:
    """Reloj monotónico + de pared controlables; ``sleep`` avanza ambos y registra."""

    def __init__(self, start_wall: datetime | None = None) -> None:
        self.mono = 1000.0
        self.wall = start_wall or datetime(2026, 9, 23, 8, 15, 34, 120000, tzinfo=BOGOTA)
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.mono

    def now(self) -> datetime:
        return self.wall

    def time(self) -> float:
        return self.wall.timestamp()

    def advance(self, seconds: float) -> None:
        from datetime import timedelta

        self.mono += seconds
        self.wall = self.wall + timedelta(seconds=seconds)

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)
