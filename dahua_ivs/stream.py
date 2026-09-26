"""Conexión a la cámara: serial, stream ``attach``, reconexión, latido propio.

Reglas (diseño B5.2, D2/D6):

- Al arrancar se lee el serial (``magicBox.cgi?action=getSerialNo``); sin serial
  NO se abre el stream.
- Stream con ``heartbeat=camera_heartbeat_seconds`` y read timeout =
  ``stream_idle_timeout`` (>= 2 x heartbeat). Sin watchdog manual: si la cámara
  calla, el read timeout corta y se reconecta.
- Reconexión con backoff 5/10/20/40/60 s (tope 60) que vuelve a 5 SOLO al recibir
  el primer ``Heartbeat`` de la conexión nueva.
- 401 (serial o attach) ⇒ ERROR claro y 15 min de espera (no martillar la
  cuenta de la cámara).
- La lectura NO usa ``iter_content(1024)``: eso bloquea hasta juntar 1024 bytes
  y retendría un ``Start`` hasta que llegara el tráfico siguiente. Se usa
  ``read1`` (lo que haya disponible).
"""

from __future__ import annotations

import http.client
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from datetime import datetime, tzinfo
from typing import Any

import requests
import urllib3
from requests.auth import HTTPDigestAuth

from dahua_ivs.audit import AuditWriter
from dahua_ivs.config import Config
from dahua_ivs.outbox import Outbox, OutboxSender
from dahua_ivs.parser import MultipartParser, boundary_from_content_type, parse_body
from dahua_ivs.records import ClockSkewMonitor, build_crossing, build_heartbeat, serialize

log = logging.getLogger("dahua_ivs.stream")

EVENT_CODE = "CrossLineDetection"
BACKOFF_DELAYS = (5, 10, 20, 40, 60)
AUTH_WAIT_SECONDS = 15 * 60
CONNECT_TIMEOUT = 10
SUMMARY_INTERVAL = 3600


class AuthError(Exception):
    """La cámara respondió 401."""


class Backoff:
    def __init__(self, delays: tuple[int, ...] = BACKOFF_DELAYS) -> None:
        self._delays = delays
        self._idx = 0

    def next(self) -> int:
        delay = self._delays[min(self._idx, len(self._delays) - 1)]
        self._idx += 1
        return delay

    def reset(self) -> None:
        self._idx = 0


def iter_available(response: Any, size: int = 4096) -> Iterator[bytes]:
    """Bytes a medida que llegan (``read1``), sin esperar a llenar un bloque.

    urllib3 2.x expone ``read1``; el urllib3 1.26 de Alpine 3.21 no, pero su
    ``_fp`` (``http.client.HTTPResponse``) sí.
    """
    raw = response.raw
    reader = getattr(raw, "read1", None)
    if reader is None:
        reader = getattr(getattr(raw, "_fp", None), "read1", None)
    if reader is None:  # pragma: no cover - no ocurre con las versiones soportadas
        log.warning("La respuesta no expone read1: se lee de a 1 byte.")
        yield from response.iter_content(chunk_size=1)
        return
    while True:
        data = reader(size)
        if not data:
            return
        yield data


def parse_serial(text: str) -> str | None:
    for line in text.replace("\r", "").splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == "sn" and value.strip():
            return value.strip()
    return None


class Counters:
    """Contadores compartidos entre el hilo del stream y el del latido."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.crossings = 0
        self.camera_heartbeats = 0
        self.reconnects = 0
        self.last_camera_heartbeat: float | None = None  # monotonic


class StreamRunner:
    def __init__(
        self,
        cfg: Config,
        *,
        audit: AuditWriter,
        outbox: Outbox | None,
        sender: OutboxSender | None,
        tz: tzinfo,
        http_get: Callable[..., Any] = requests.get,
        sleep: Callable[[float], Any] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.cfg = cfg
        self.audit = audit
        self.outbox = outbox
        self.sender = sender
        self.tz = tz
        self._get = http_get
        self._sleep = sleep
        self._monotonic = monotonic
        self._now = now or (lambda: datetime.now().astimezone())
        self._auth = HTTPDigestAuth(cfg.camera_user, cfg.camera_password)
        self.backoff = Backoff()
        self.counters = Counters()
        self.skew = ClockSkewMonitor(monotonic=monotonic)
        self.serial: str | None = None
        self._attempts = 0
        self._first_heartbeat_pending = False

    # -- URLs -------------------------------------------------------------
    @property
    def serial_url(self) -> str:
        return f"http://{self.cfg.camera_host}/cgi-bin/magicBox.cgi?action=getSerialNo"

    @property
    def attach_url(self) -> str:
        return (
            f"http://{self.cfg.camera_host}/cgi-bin/eventManager.cgi?action=attach"
            f"&codes=%5B{EVENT_CODE}%5D&heartbeat={self.cfg.camera_heartbeat_seconds}"
        )

    # -- pasos ------------------------------------------------------------
    def fetch_serial(self) -> str:
        resp = self._get(self.serial_url, auth=self._auth, timeout=(CONNECT_TIMEOUT, 10))
        if resp.status_code == 401:
            raise AuthError("getSerialNo")
        resp.raise_for_status()
        serial = parse_serial(resp.text)
        if not serial:
            raise ValueError(f"respuesta de getSerialNo sin 'sn=': {resp.text[:80]!r}")
        return serial

    def handle_part_body(self, body_bytes: bytes) -> None:
        body = parse_body(body_bytes)
        if body is None:
            return
        if body.heartbeat:
            with self.counters.lock:
                self.counters.camera_heartbeats += 1
                self.counters.last_camera_heartbeat = self._monotonic()
                first = self._first_heartbeat_pending
                self._first_heartbeat_pending = False
            if first:
                self.backoff.reset()
                log.info("Stream sano: primer Heartbeat de la cámara recibido.")
            return
        if body.code != EVENT_CODE or body.action != "Start":
            log.debug("Ignorado: Code=%s action=%s", body.code, body.action)
            return
        received_at = self._now()
        record = build_crossing(
            body,
            device_serial=self.serial or "",
            device_ip=self.cfg.camera_host,
            received_at=received_at,
            tz=self.tz,
        )
        line = serialize(record)
        self.emit(line)
        with self.counters.lock:
            self.counters.crossings += 1
        self.skew.check(record["device_ts"], record["received_ts"])
        log.debug("Cruce: %s", line)

    def emit(self, line: str) -> None:
        """Mismo string a la auditoría y a la cola (si el fan-out está activo)."""
        self.audit.write(line)
        if self.outbox is not None:
            try:
                self.outbox.put(line)
            except sqlite3.Error as exc:
                # La auditoría ya lo tiene; un disco lleno no debe cortar el stream.
                log.error("No se pudo encolar el registro (%s); queda solo en la auditoría.", exc)
                return
            if self.sender is not None:
                self.sender.notify()

    def read_stream(self) -> None:
        """Abre el attach y procesa hasta que se corte. Levanta en errores."""
        resp = self._get(
            self.attach_url,
            auth=self._auth,
            stream=True,
            timeout=(CONNECT_TIMEOUT, self.cfg.stream_idle_timeout),
        )
        try:
            if resp.status_code == 401:
                raise AuthError("attach")
            resp.raise_for_status()
            boundary = boundary_from_content_type(resp.headers.get("Content-Type"))
            parser = MultipartParser(boundary)
            self._first_heartbeat_pending = True
            log.info("Conectado al stream de eventos (boundary=%s).", boundary)
            for chunk in iter_available(resp):
                for part in parser.feed(chunk):
                    self.handle_part_body(part.body)
            log.warning("La cámara cerró el stream.")
        finally:
            resp.close()

    def run_once(self) -> None:
        """Un ciclo: asegura el serial, abre el stream y espera el tiempo que toque."""
        try:
            if self.serial is None:
                self.serial = self.fetch_serial()
                log.info("Serial de la cámara: %s", self.serial)
            if self._attempts > 0:
                with self.counters.lock:
                    self.counters.reconnects += 1
            self._attempts += 1
            log.info("Conectando a %s", self.attach_url)
            self.read_stream()
        except AuthError as exc:
            log.error(
                "*** La cámara rechazó usuario/clave (401 en %s). Revisar camera_user / "
                "camera_password. Espera de %d min antes de reintentar (no martillar la cuenta). ***",
                exc,
                AUTH_WAIT_SECONDS // 60,
            )
            self._sleep(AUTH_WAIT_SECONDS)
            return
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            log.warning("Stream cortado o sin datos (%s).", _short(exc))
        except TimeoutError as exc:
            log.warning("Stream sin datos durante %d s (%s).", self.cfg.stream_idle_timeout, _short(exc))
        except requests.exceptions.HTTPError as exc:
            log.error("La cámara respondió error HTTP: %s", _short(exc))
        except requests.RequestException as exc:
            log.error("Error de request: %s", _short(exc))
        except (OSError, http.client.HTTPException) as exc:
            log.warning("Stream cortado (%s).", _short(exc))
        except Exception as exc:
            if _is_read_timeout(exc):
                log.warning("Stream sin datos durante %d s.", self.cfg.stream_idle_timeout)
            elif _is_protocol_error(exc):
                log.warning("Stream cortado (%s).", _short(exc))
            else:
                log.exception("Excepción no esperada en el stream: %s", exc)
        delay = self.backoff.next()
        log.info("Reintentando en %d s.", delay)
        self._sleep(delay)

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.run_once()

    # -- latido propio y resumen ------------------------------------------
    def stream_healthy(self) -> bool:
        with self.counters.lock:
            last = self.counters.last_camera_heartbeat
        return last is not None and self._monotonic() - last < self.cfg.stream_idle_timeout


def _short(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}"


def _is_read_timeout(exc: BaseException) -> bool:
    return isinstance(exc, urllib3.exceptions.ReadTimeoutError)


def _is_protocol_error(exc: BaseException) -> bool:
    return isinstance(exc, urllib3.exceptions.ProtocolError)


class Ticker:
    """Latido propio cada ``heartbeat_interval_minutes`` (solo con stream sano) y resumen horario."""

    def __init__(
        self,
        runner: StreamRunner,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.runner = runner
        self._monotonic = monotonic
        self._now = now or (lambda: datetime.now().astimezone())
        self._interval = runner.cfg.heartbeat_interval_minutes * 60
        start = monotonic()
        self._next_heartbeat = start + self._interval
        self._next_summary = start + SUMMARY_INTERVAL
        # Contadores "desde el último latido" y "desde el último resumen".
        self._hb_base = (0, 0, 0)
        self._sum_base = (0, 0, 0)

    def _snapshot(self) -> tuple[int, int, int]:
        c = self.runner.counters
        with c.lock:
            return (c.crossings, c.camera_heartbeats, c.reconnects)

    def _pending(self) -> int | None:
        return self.runner.outbox.pending_count() if self.runner.outbox is not None else None

    def tick(self) -> None:
        now = self._monotonic()
        heartbeat_due = now >= self._next_heartbeat
        summary_due = now >= self._next_summary
        if not (heartbeat_due or summary_due):
            return
        # Foto de la cola ANTES de encolar el latido de esta vuelta (#28). El latido y el
        # resumen vencen en la misma vuelta (3600 s = 4 x 900 s y se reprograman desde el
        # mismo ``now``): contar después del ``emit`` veía siempre el latido recién
        # encolado, que el hilo de envío todavía no llegó a mandar. Un registro que de
        # verdad no drena sigue en la cola en cualquier momento, así que sigue contando.
        pending = self._pending()
        if heartbeat_due:
            if self.runner.stream_healthy():
                snap = self._snapshot()
                delta = tuple(a - b for a, b in zip(snap, self._hb_base, strict=True))
                record = build_heartbeat(
                    device_serial=self.runner.serial or "",
                    device_ip=self.runner.cfg.camera_host,
                    received_at=self._now(),
                    crossings_since_last=delta[0],
                    camera_heartbeats_since_last=delta[1],
                    reconnects_since_last=delta[2],
                    outbox_pending=pending,
                )
                self.runner.emit(serialize(record))
                self._hb_base = snap
                self._next_heartbeat = now + self._interval
                log.debug("Latido propio emitido: %s", record)
            else:
                # Queda pendiente: sale en cuanto el stream vuelva a estar sano.
                log.debug("Latido propio pendiente: stream no sano.")
        if summary_due:
            snap = self._snapshot()
            d = tuple(a - b for a, b in zip(snap, self._sum_base, strict=True))
            log.info(
                "Resumen última hora: cruces=%d latidos_camara=%d reconexiones=%d pendientes_cola=%s",
                d[0],
                d[1],
                d[2],
                "n/a (fan-out apagado)" if pending is None else pending,
            )
            self._sum_base = snap
            self._next_summary = now + SUMMARY_INTERVAL

    def run(self, stop: threading.Event) -> None:
        while not stop.wait(5.0):
            try:
                self.tick()
            except Exception:
                log.exception("Error en el latido/resumen.")
