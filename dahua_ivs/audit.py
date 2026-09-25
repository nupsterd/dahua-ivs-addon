"""Auditoría local en JSON Lines, un archivo por día, con retención.

``<audit_dir>/dahua_ivs_audit_YYYYMMDD.jsonl`` (fecha local de la Pi). Cada línea
es EXACTAMENTE la cadena que va (o iría) en el POST. Flush por línea. Los
archivos con más de ``audit_retention_days`` días se borran al arrancar y en
cada cambio de día.
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TextIO

log = logging.getLogger("dahua_ivs.audit")

PREFIX = "dahua_ivs_audit_"
_NAME_RE = re.compile(r"^dahua_ivs_audit_(\d{8})\.jsonl$")


class AuditWriter:
    def __init__(
        self,
        directory: str | Path,
        retention_days: int,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._dir = Path(directory)
        self._retention = retention_days
        self._now = now or (lambda: datetime.now().astimezone())
        self._lock = threading.Lock()
        self._fh: TextIO | None = None
        self._day: date | None = None

    def path_for(self, day: date) -> Path:
        return self._dir / f"{PREFIX}{day:%Y%m%d}.jsonl"

    def write(self, line: str) -> None:
        """Agrega una línea. Un error de disco se loguea y NO corta el flujo."""
        with self._lock:
            try:
                today = self._now().date()
                if self._fh is None or today != self._day:
                    self._rotate(today)
                assert self._fh is not None
                self._fh.write(line + "\n")
                self._fh.flush()
            except OSError as exc:
                log.error("Falló la escritura de la auditoría en %s: %s", self._dir, exc)

    def _rotate(self, today: date) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        self._dir.mkdir(parents=True, exist_ok=True)
        self._fh = self.path_for(today).open("a", encoding="utf-8")
        self._day = today
        self._purge(today)

    def purge(self) -> list[Path]:
        with self._lock:
            return self._purge(self._now().date())

    def _purge(self, today: date) -> list[Path]:
        limit = today - timedelta(days=self._retention)
        borrados: list[Path] = []
        if not self._dir.is_dir():
            return borrados
        for p in sorted(self._dir.iterdir()):
            m = _NAME_RE.match(p.name)
            if not m:
                continue
            try:
                day = datetime.strptime(m.group(1), "%Y%m%d").date()
            except ValueError:
                continue
            if day < limit:
                try:
                    p.unlink()
                    borrados.append(p)
                except OSError as exc:
                    log.error("No se pudo borrar la auditoría vieja %s: %s", p, exc)
        if borrados:
            log.info(
                "Retención de auditoría: %d archivo(s) de más de %d días borrados.",
                len(borrados),
                self._retention,
            )
        return borrados

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.close()
                self._fh = None
