# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
versionado siguiendo [SemVer](https://semver.org/lang/es/).

## [0.1.1-alpha] - 2026-09-26

### Fixed
- Resumen horario con `pendientes_cola=1` fijo desde que se encendió el fan-out (#28).
  El latido propio (cada 15 min) y el resumen (cada 60 min) vencen en la misma vuelta,
  y el resumen contaba la cola justo después de encolar el latido, antes de que el hilo
  de envío lo mandara. Ahora la cola se cuenta UNA vez por vuelta, antes de encolar el
  latido, y ese mismo número va al resumen y al `outbox_pending` del latido. Un registro
  que no drena sigue contando.

## [0.1.0-alpha] - 2026-09-25

Primera versión (B5.2). Pendiente del smoke en hardware real antes del tag.

### Added
- Consumidor del stream `eventManager.cgi?action=attach` de cámaras Dahua WizMind
  (digest), código suscrito fijo `CrossLineDetection`.
- Lectura del serial de la cámara al arrancar (`magicBox.cgi?action=getSerialNo`);
  sin serial no se abre el stream.
- Parser multipart que entrega cada parte al completar sus `Content-Length`
  bytes (sin esperar la parte siguiente, corrige el defecto §5.9.586 de los
  add-ons Hikvision) y lectura con `read1` en lugar de bloques de 1024 bytes.
  Fallback al boundary siguiente si una parte no trae `Content-Length`.
- Registro de cruce recortado (solo `action=Start`, ambos sentidos) con
  `device_ts` corregido (`UTC` de la cámara = hora local, §5.9.625d),
  `received_ts` y `device_utc_raw`. Aviso de reloj desfasado (> 120 s, máx. cada 10 min).
- Latido propio cada `heartbeat_interval_minutes`, solo con el stream sano, y
  resumen INFO por hora.
- Reconexión con backoff 5/10/20/40/60 s que vuelve a 5 con el primer Heartbeat;
  401 ⇒ ERROR y espera de 15 min.
- Auditoría JSON Lines diaria con retención.
- Fan-out opcional al backend (`X-PV-Dahua-Token`) con cola SQLite persistente,
  FIFO, límites por cantidad y edad, clasificación de respuestas (borrar /
  reintentar / pausa por configuración / tabla `failed`).
- Suite de tests con capturas reales, incluida una prueba contra un servidor
  HTTP real en loopback.
