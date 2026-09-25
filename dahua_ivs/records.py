"""Registros que produce el add-on (cruce y latido) y su serialización única.

El registro se serializa UNA sola vez (``serialize``) y esa misma cadena va a la
auditoría y a la cola: el POST reenvía siempre exactamente los mismos bytes, así
que el backend puede deduplicar con confianza.

Llave de idempotencia de un cruce para el backend (B5.3):
``device_serial + rule_id + object_id + device_ts``.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta, tzinfo
from typing import Any

from dahua_ivs import ADDON_VERSION, DEVICE_KIND
from dahua_ivs.parser import Body

log = logging.getLogger("dahua_ivs.records")

_EPOCH = datetime(1970, 1, 1)

CROSSING_FIELDS = (
    "device_kind",
    "kind",
    "device_serial",
    "device_ip",
    "device_ts",
    "received_ts",
    "device_utc_raw",
    "channel",
    "rule_name",
    "rule_id",
    "event_id",
    "direction",
    "object_id",
    "object_type",
    "bbox",
    "center",
    "addon_version",
)

HEARTBEAT_FIELDS = (
    "device_kind",
    "kind",
    "device_serial",
    "device_ip",
    "received_ts",
    "crossings_since_last",
    "camera_heartbeats_since_last",
    "reconnects_since_last",
    "outbox_pending",
    "addon_version",
)


def iso_ms(dt: datetime) -> str:
    """ISO 8601 con offset y milisegundos: ``2026-09-23T08:15:33.340-05:00``."""
    return dt.isoformat(timespec="milliseconds")


def device_datetime(utc: int, utcms: int, tz: tzinfo) -> datetime:
    """Hora del cruce según la cámara.

    El campo ``UTC`` de la Dahua NO es UTC: es la hora de pared local codificada
    como epoch (§5.9.625d; ``1790151333`` = 08:15:33 en Bogotá). Se interpreta
    como hora de pared y se le pone la zona del sitio (``camera_timezone``).
    """
    wall = _EPOCH + timedelta(seconds=utc, milliseconds=utcms)
    return wall.replace(tzinfo=tz)


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def build_crossing(
    body: Body,
    *,
    device_serial: str,
    device_ip: str,
    received_at: datetime,
    tz: tzinfo,
) -> dict[str, Any]:
    """Recorta un ``action=Start`` al registro de cruce (sin ``raw``)."""
    data = body.data or {}
    obj = data.get("Object") if isinstance(data.get("Object"), dict) else {}
    utc = _int_or_none(data.get("UTC"))
    utcms = _int_or_none(data.get("UTCMS"))
    device_ts = None
    if utc is not None:
        device_ts = iso_ms(device_datetime(utc, utcms or 0, tz))
    name = data.get("Name")
    index = body.fields.get("index", "")
    return {
        "device_kind": DEVICE_KIND,
        "kind": "crossing",
        "device_serial": device_serial,
        "device_ip": device_ip,
        "device_ts": device_ts,
        "received_ts": iso_ms(received_at),
        "device_utc_raw": {"utc": utc, "utcms": utcms},
        "channel": int(index) if index.isdigit() else None,
        "rule_name": name.strip() if isinstance(name, str) else None,
        "rule_id": data.get("RuleID"),
        "event_id": data.get("EventID"),
        "direction": data.get("Direction"),
        "object_id": obj.get("ObjectID"),
        "object_type": obj.get("ObjectType"),
        "bbox": obj.get("BoundingBox"),
        "center": obj.get("Center"),
        "addon_version": ADDON_VERSION,
    }


def build_heartbeat(
    *,
    device_serial: str,
    device_ip: str,
    received_at: datetime,
    crossings_since_last: int,
    camera_heartbeats_since_last: int,
    reconnects_since_last: int,
    outbox_pending: int | None,
) -> dict[str, Any]:
    """Latido propio del add-on (solo se emite con el stream sano)."""
    return {
        "device_kind": DEVICE_KIND,
        "kind": "heartbeat",
        "device_serial": device_serial,
        "device_ip": device_ip,
        "received_ts": iso_ms(received_at),
        "crossings_since_last": crossings_since_last,
        "camera_heartbeats_since_last": camera_heartbeats_since_last,
        "reconnects_since_last": reconnects_since_last,
        "outbox_pending": outbox_pending,
        "addon_version": ADDON_VERSION,
    }


def serialize(record: dict[str, Any]) -> str:
    """Serialización única y estable (una línea, sin espacios, orden de inserción)."""
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


class ClockSkewMonitor:
    """WARNING si |device_ts - received_ts| supera el umbral, como máximo 1 vez por intervalo."""

    def __init__(
        self,
        threshold_seconds: float = 120.0,
        interval_seconds: float = 600.0,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self._threshold = threshold_seconds
        self._interval = interval_seconds
        self._monotonic = monotonic or time.monotonic
        self._last_warn: float | None = None

    def check(self, device_ts: str | None, received_ts: str) -> bool:
        """True si se emitió el WARNING."""
        if device_ts is None:
            return False
        skew = (datetime.fromisoformat(device_ts) - datetime.fromisoformat(received_ts)).total_seconds()
        if abs(skew) <= self._threshold:
            return False
        now = self._monotonic()
        if self._last_warn is not None and now - self._last_warn < self._interval:
            return False
        self._last_warn = now
        log.warning(
            "Reloj desfasado: device_ts=%s received_ts=%s (diferencia %.0f s > %.0f s). "
            "Revisar NTP y zona de la cámara (camera_timezone). Aviso cada %d min como máximo.",
            device_ts,
            received_ts,
            skew,
            self._threshold,
            int(self._interval // 60),
        )
        return True
