"""Configuración del add-on, leída de ``/data/options.json`` (la escribe el Supervisor).

Los nombres, tipos y defaults de ``Config`` son los mismos que ``options`` de
``config.yaml`` (un test lo verifica cargando el YAML).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

OPTIONS_PATH = "/data/options.json"
LOG_LEVELS = ("debug", "info", "warning")


@dataclass(frozen=True)
class Config:
    camera_host: str
    camera_user: str = "admin"
    # repr=False: un log de la config o un traceback nunca imprime secretos.
    camera_password: str = field(default="", repr=False)
    camera_timezone: str = "America/Bogota"
    camera_heartbeat_seconds: int = 30
    stream_idle_timeout: int = 75
    heartbeat_interval_minutes: int = 15
    backend_url: str = ""
    backend_secret: str = field(default="", repr=False)
    backend_timeout_seconds: int = 5
    outbox_max_records: int = 100000
    outbox_max_age_days: int = 7
    audit_dir: str = "/config"
    audit_retention_days: int = 30
    log_level: str = "info"

    @classmethod
    def from_dict(cls, opts: dict[str, Any]) -> Config:
        kwargs: dict[str, Any] = {}
        for f in fields(cls):
            if f.name not in opts or opts[f.name] is None:
                continue
            value = opts[f.name]
            kwargs[f.name] = int(value) if f.type == "int" else str(value)
        return cls(**kwargs)

    @classmethod
    def from_options_json(cls, path: str = OPTIONS_PATH) -> Config:
        with open(path, encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))

    @property
    def backend_enabled(self) -> bool:
        return bool(self.backend_url.strip())

    def validate(self) -> list[str]:
        """Errores de configuración que impiden arrancar (lista vacía = OK)."""
        errores: list[str] = []
        if not self.camera_host.strip():
            errores.append("camera_host está vacío")
        if self.stream_idle_timeout < 2 * self.camera_heartbeat_seconds:
            errores.append(
                f"stream_idle_timeout ({self.stream_idle_timeout} s) debe ser >= 2 x "
                f"camera_heartbeat_seconds ({2 * self.camera_heartbeat_seconds} s)"
            )
        try:
            ZoneInfo(self.camera_timezone)
        except (ZoneInfoNotFoundError, ValueError):
            errores.append(f"camera_timezone desconocida: {self.camera_timezone!r}")
        if self.log_level not in LOG_LEVELS:
            errores.append(f"log_level inválido: {self.log_level!r}")
        return errores

    def describe(self) -> dict[str, Any]:
        """Config para el log de arranque: los secretos solo como configurado/vacío."""
        out: dict[str, Any] = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if f.name in ("camera_password", "backend_secret"):
                value = "configurado" if value else "vacío"
            elif f.name == "backend_url" and not value:
                value = "(fan-out apagado)"
            out[f.name] = value
        return out
