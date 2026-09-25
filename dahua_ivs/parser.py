"""Parser incremental del multipart de ``eventManager.cgi?action=attach``.

Forma real del stream (capturas del 23-sep, §5.9.625)::

    --myboundary\\r\\n
    Content-Type: text/plain\\r\\n
    Content-Length: 9\\r\\n
    \\r\\n
    Heartbeat\\r\\n
    \\r\\n
    --myboundary\\r\\n
    Content-Type: text/plain\\r\\n
    Content-Length: 1102\\r\\n
    \\r\\n
    Code=CrossLineDetection;action=Start;index=0;data={\\n ...JSON multilínea... \\n}\\n
    \\r\\n

Diferencia clave con el parser del add-on facial (§5.9.586): la parte se
ENTREGA apenas llegan sus ``Content-Length`` bytes, sin esperar el boundary
siguiente. Solo si una parte no trae ``Content-Length`` se cae al modo viejo
(entregar al ver el boundary siguiente), con un WARNING la primera vez.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("dahua_ivs.parser")

DEFAULT_BOUNDARY = "myboundary"
# Defensas contra un stream corrupto: nunca se acumula sin límite.
_MAX_HEADER_BYTES = 8 * 1024
_MAX_PART_BYTES = 1024 * 1024
_BLANK_LINE = re.compile(rb"\r?\n\r?\n")


def boundary_from_content_type(content_type: str | None) -> str:
    """Boundary del header ``Content-Type`` de la respuesta; ``myboundary`` si no viene."""
    if content_type:
        m = re.search(r'boundary\s*=\s*"?([^";\s]+)"?', content_type, re.IGNORECASE)
        if m:
            value = m.group(1)
            return value[2:] if value.startswith("--") else value
    return DEFAULT_BOUNDARY


@dataclass(frozen=True)
class Part:
    headers: dict[str, str]
    body: bytes


class MultipartParser:
    """Recibe bytes en trozos de cualquier tamaño y devuelve las partes completas."""

    _SEEK, _HEADERS, _BODY_CL, _BODY_BOUNDARY = range(4)

    def __init__(self, boundary: str = DEFAULT_BOUNDARY) -> None:
        self._marker = b"--" + boundary.encode("ascii")
        self._buf = b""
        self._state = self._SEEK
        self._headers: dict[str, str] = {}
        self._length = 0
        self._warned_no_length = False

    def feed(self, data: bytes) -> list[Part]:
        self._buf += data
        parts: list[Part] = []
        while True:
            if self._state == self._SEEK:
                if not self._seek_boundary():
                    break
            elif self._state == self._HEADERS:
                if not self._read_headers():
                    break
            elif self._state == self._BODY_CL:
                if len(self._buf) < self._length:
                    break
                parts.append(Part(self._headers, self._buf[: self._length]))
                self._buf = self._buf[self._length :]
                self._state = self._SEEK
            else:  # _BODY_BOUNDARY
                idx = self._buf.find(self._marker)
                if idx < 0:
                    if len(self._buf) > _MAX_PART_BYTES:
                        log.warning("Parte sin Content-Length supera %d bytes: descartada", _MAX_PART_BYTES)
                        self._reset()
                    break
                body = self._buf[:idx]
                if body.endswith(b"\r\n"):
                    body = body[:-2]
                elif body.endswith(b"\n"):
                    body = body[:-1]
                parts.append(Part(self._headers, body))
                self._buf = self._buf[idx:]
                self._state = self._SEEK
        return parts

    def _reset(self) -> None:
        self._buf = b""
        self._state = self._SEEK

    def _seek_boundary(self) -> bool:
        idx = self._buf.find(self._marker)
        if idx < 0:
            # Conservar la cola por si el marcador quedó partido entre dos trozos.
            keep = len(self._marker) - 1
            if len(self._buf) > keep:
                self._buf = self._buf[-keep:]
            return False
        eol = self._buf.find(b"\n", idx + len(self._marker))
        if eol < 0:
            self._buf = self._buf[idx:]
            return False
        self._buf = self._buf[eol + 1 :]
        self._state = self._HEADERS
        return True

    def _read_headers(self) -> bool:
        # Parte sin headers: la línea en blanco viene inmediatamente.
        if self._buf.startswith(b"\r\n") or self._buf.startswith(b"\n"):
            raw_headers, end = b"", (2 if self._buf.startswith(b"\r\n") else 1)
        else:
            m = _BLANK_LINE.search(self._buf)
            if m is None:
                if len(self._buf) > _MAX_HEADER_BYTES:
                    log.warning("Headers de parte sin fin tras %d bytes: resincronizando", _MAX_HEADER_BYTES)
                    self._reset()
                return False
            raw_headers, end = self._buf[: m.start()], m.end()
        self._buf = self._buf[end:]
        headers: dict[str, str] = {}
        for line in raw_headers.decode("latin-1").splitlines():
            name, sep, value = line.partition(":")
            if sep:
                headers[name.strip().lower()] = value.strip()
        self._headers = headers
        length = headers.get("content-length")
        if length is not None and length.isdigit() and int(length) <= _MAX_PART_BYTES:
            self._length = int(length)
            self._state = self._BODY_CL
        else:
            if not self._warned_no_length:
                log.warning(
                    "Parte sin Content-Length válido (%r): se entrega al llegar el boundary "
                    "siguiente (más lento). Aviso único.",
                    length,
                )
                self._warned_no_length = True
            self._state = self._BODY_BOUNDARY
        return True


@dataclass(frozen=True)
class Body:
    """Cuerpo interpretado de una parte."""

    heartbeat: bool
    fields: dict[str, str]  # Code / action / index ...
    data: dict[str, Any] | None

    @property
    def code(self) -> str | None:
        return self.fields.get("Code")

    @property
    def action(self) -> str | None:
        return self.fields.get("action")


def parse_body(body: bytes) -> Body | None:
    """``Heartbeat`` o ``Code=...;action=...;index=...;data={JSON}``. ``None`` si no se entiende."""
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    if text == "Heartbeat":
        return Body(heartbeat=True, fields={}, data=None)
    head, sep, rest = text.partition("data=")
    kv: dict[str, str] = {}
    for item in head.split(";"):
        key, eq, value = item.partition("=")
        if eq:
            kv[key.strip()] = value.strip()
    if "Code" not in kv:
        log.warning("Cuerpo no reconocido: %r", text[:100])
        return None
    data: dict[str, Any] | None = None
    if sep:
        try:
            loaded = json.loads(rest)
        except json.JSONDecodeError as exc:
            log.warning("JSON de data inválido (%s) en %r", exc, text[:100])
            return None
        data = loaded if isinstance(loaded, dict) else None
    return Body(heartbeat=False, fields=kv, data=data)
