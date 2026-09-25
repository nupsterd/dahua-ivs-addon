"""Cola persistente (SQLite, WAL) y envío FIFO al backend.

Solo existe si ``backend_url`` está configurado. Sobrevive a reinicios del
add-on y de la Pi (``/data/outbox.sqlite``, ``synchronous=FULL``). Un único hilo
drena en orden: el registro de la cabeza no se salta salvo que el backend lo
rechace por contenido (400/422 ⇒ tabla ``failed``).

Clasificación de respuestas del backend:

======================================  =========================================
Respuesta                               Acción
======================================  =========================================
2xx con ``status`` received/duplicate   borrar de la cola
2xx con ``status`` device_unknown       ERROR DE CONFIGURACIÓN (pausa, no borra)
2xx con otro ``status`` o sin JSON      ERROR DE CONFIGURACIÓN (pausa, no borra)
401, 403, 404, 405                      ERROR DE CONFIGURACIÓN (pausa, no borra)
400, 413, 422 y otros 4xx               mover a ``failed`` (código + motivo)
408, 429, 5xx, timeout, error de red    reintento con backoff 5 s → 5 min
======================================  =========================================
"""

from __future__ import annotations

import enum
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger("dahua_ivs.outbox")

OUTBOX_PATH = "/data/outbox.sqlite"
AUTH_HEADER = "X-PV-Dahua-Token"
RETRY_DELAYS = (5, 10, 20, 40, 80, 160, 300)
CONFIG_ERROR_DELAY = 600  # ERROR y reintento cada 10 min
DROP_WARN_INTERVAL = 600

_SCHEMA = """
CREATE TABLE IF NOT EXISTS pending (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at REAL    NOT NULL,
    payload    TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS failed (
    id          INTEGER PRIMARY KEY,
    created_at  REAL    NOT NULL,
    failed_at   REAL    NOT NULL,
    status_code INTEGER,
    reason      TEXT,
    payload     TEXT    NOT NULL
);
"""


class Outbox:
    """Cola FIFO en SQLite. Thread-safe (un lock para la única conexión)."""

    def __init__(
        self,
        path: str | Path,
        max_records: int,
        max_age_days: int,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._max_records = max_records
        self._max_age = max_age_days * 86400
        self._clock = clock
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._dropped_since_warn = 0
        self._last_drop_warn: float | None = None

    def put(self, payload: str) -> int:
        """Encola y aplica los límites. Devuelve cuántos registros viejos se descartaron."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO pending (created_at, payload) VALUES (?, ?)",
                (self._clock(), payload),
            )
            return self._enforce_limits_locked()

    def enforce_limits(self) -> int:
        with self._lock:
            return self._enforce_limits_locked()

    def _enforce_limits_locked(self) -> int:
        cutoff = self._clock() - self._max_age
        dropped = self._conn.execute("DELETE FROM pending WHERE created_at < ?", (cutoff,)).rowcount
        (count,) = self._conn.execute("SELECT COUNT(*) FROM pending").fetchone()
        excess = count - self._max_records
        if excess > 0:
            dropped += self._conn.execute(
                "DELETE FROM pending WHERE id IN (SELECT id FROM pending ORDER BY id LIMIT ?)",
                (excess,),
            ).rowcount
        if dropped:
            self._warn_dropped(dropped)
        return dropped

    def _warn_dropped(self, n: int) -> None:
        self._dropped_since_warn += n
        now = self._monotonic()
        if self._last_drop_warn is None or now - self._last_drop_warn >= DROP_WARN_INTERVAL:
            log.warning(
                "Cola llena o vencida (máx %d registros / %d días): %d registro(s) MÁS VIEJOS "
                "descartados desde el último aviso.",
                self._max_records,
                self._max_age // 86400,
                self._dropped_since_warn,
            )
            self._dropped_since_warn = 0
            self._last_drop_warn = now

    def peek(self) -> tuple[int, str] | None:
        with self._lock:
            row = self._conn.execute("SELECT id, payload FROM pending ORDER BY id LIMIT 1").fetchone()
        return (row[0], row[1]) if row else None

    def delete(self, record_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM pending WHERE id = ?", (record_id,))

    def move_to_failed(self, record_id: int, status_code: int | None, reason: str) -> None:
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    "INSERT INTO failed (id, created_at, failed_at, status_code, reason, payload) "
                    "SELECT id, created_at, ?, ?, ?, payload FROM pending WHERE id = ?",
                    (self._clock(), status_code, reason[:500], record_id),
                )
                self._conn.execute("DELETE FROM pending WHERE id = ?", (record_id,))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def pending_count(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM pending").fetchone()[0])

    def failed_rows(self) -> list[tuple[int, int | None, str, str]]:
        with self._lock:
            return list(
                self._conn.execute("SELECT id, status_code, reason, payload FROM failed ORDER BY id")
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class Action(enum.Enum):
    DELETE = "delete"
    RETRY = "retry"
    CONFIG_ERROR = "config_error"
    FAIL_RECORD = "fail_record"


@dataclass(frozen=True)
class Verdict:
    action: Action
    status_code: int | None
    reason: str


def classify(status_code: int, body_text: str) -> Verdict:
    """Decide qué hacer con la respuesta del backend (ver tabla del módulo)."""
    if 200 <= status_code < 300:
        try:
            status = json.loads(body_text).get("status")
        except (ValueError, AttributeError):
            status = None
        if status in ("received", "duplicate"):
            return Verdict(Action.DELETE, status_code, str(status))
        if status == "device_unknown":
            return Verdict(
                Action.CONFIG_ERROR,
                status_code,
                "device_unknown: la cámara no está dada de alta en el backend con este serial",
            )
        return Verdict(Action.CONFIG_ERROR, status_code, f"respuesta 2xx no reconocida: {body_text[:200]!r}")
    if status_code in (401, 403):
        return Verdict(Action.CONFIG_ERROR, status_code, "token rechazado (revisar backend_secret)")
    if status_code in (404, 405):
        return Verdict(
            Action.CONFIG_ERROR,
            status_code,
            "endpoint inexistente (backend_url debe ser el endpoint COMPLETO, §5.9.579)",
        )
    if status_code in (408, 429) or status_code >= 500:
        return Verdict(Action.RETRY, status_code, f"HTTP {status_code}")
    return Verdict(Action.FAIL_RECORD, status_code, f"HTTP {status_code}: {body_text[:300]}")


PostFn = Callable[[str, bytes, dict[str, str], float], Any]


def _requests_post(url: str, data: bytes, headers: dict[str, str], timeout: float) -> Any:
    return requests.post(url, data=data, headers=headers, timeout=timeout)


class OutboxSender:
    """Drena la cola en orden. ``process_one`` es un paso (testeable); ``run`` el bucle."""

    def __init__(
        self,
        outbox: Outbox,
        url: str,
        secret: str,
        timeout_seconds: float,
        post: PostFn = _requests_post,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._outbox = outbox
        self._url = url.strip()
        self._secret = secret
        self._timeout = timeout_seconds
        self._post = post
        self._monotonic = monotonic
        self._retry_idx = 0
        self._wakeup = threading.Event()
        self.paused_reason: str | None = None

    def notify(self) -> None:
        """Despierta al hilo cuando se encola algo."""
        self._wakeup.set()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._secret:
            headers[AUTH_HEADER] = self._secret
        return headers

    def process_one(self) -> float | None:
        """Intenta enviar la cabeza de la cola.

        Devuelve cuántos segundos esperar antes del siguiente paso: ``0`` para
        seguir de inmediato, ``None`` si la cola está vacía.
        """
        self._outbox.enforce_limits()
        head = self._outbox.peek()
        if head is None:
            return None
        record_id, payload = head
        try:
            resp = self._post(self._url, payload.encode("utf-8"), self._headers(), self._timeout)
            verdict = classify(resp.status_code, resp.text)
        except requests.RequestException as exc:
            verdict = Verdict(Action.RETRY, None, f"{type(exc).__name__}: {exc}")

        if verdict.action is Action.DELETE:
            self._outbox.delete(record_id)
            self._retry_idx = 0
            if self.paused_reason is not None:
                log.info("Envío al backend reanudado: la configuración volvió a ser aceptada.")
                self.paused_reason = None
            return 0.0
        if verdict.action is Action.FAIL_RECORD:
            self._outbox.move_to_failed(record_id, verdict.status_code, verdict.reason)
            log.error(
                "Registro %d rechazado por el backend (%s): movido a la tabla failed; se sigue con el próximo.",
                record_id,
                verdict.reason,
            )
            return 0.0
        if verdict.action is Action.CONFIG_ERROR:
            self.paused_reason = verdict.reason
            log.error(
                "ERROR DE CONFIGURACIÓN del fan-out (HTTP %s): %s. Envío PAUSADO sin descartar nada "
                "(%d en cola); reintento en %d min.",
                verdict.status_code,
                verdict.reason,
                self._outbox.pending_count(),
                CONFIG_ERROR_DELAY // 60,
            )
            return float(CONFIG_ERROR_DELAY)
        delay = RETRY_DELAYS[min(self._retry_idx, len(RETRY_DELAYS) - 1)]
        self._retry_idx += 1
        log.warning(
            "Backend no disponible (%s): reintento del registro %d en %d s (%d en cola).",
            verdict.reason,
            record_id,
            delay,
            self._outbox.pending_count(),
        )
        return float(delay)

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                delay = self.process_one()
            except Exception:
                log.exception("Error inesperado en el envío al backend; reintento en 30 s.")
                delay = 30.0
            if delay is None:
                self._wakeup.wait(timeout=30.0)
                self._wakeup.clear()
            elif delay > 0:
                stop.wait(delay)
