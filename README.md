# Dahua IVS (Portería Virtual) — Home Assistant Add-on

Add-on de Home Assistant OS que corre en la Pi de cada sitio y consume el stream
de eventos de una cámara **Dahua WizMind** (probado con `DH-IPC-HFW5242HN-ZHE-MF`,
firmware `2.840.0000000.18.R`) con una regla **tripwire** (escena IVS,
`CrossLineDetection`).

**Es un mensajero:** no correlaciona nada. Por cada cruce toma solo el
`action=Start`, lo recorta a un registro plano, lo escribe en la auditoría local
y, si hay backend configurado, lo reenvía con una cola persistente. La
correlación con marcaciones y aperturas (anti-tailgating) la hace el backend.

```
 ┌──────────────┐  attach (digest)  ┌──────────────────────────┐  POST JSON   ┌────────────┐
 │ Cámara Dahua │ ────────────────▶ │ este add-on (Pi)         │ ───────────▶ │ pv-backend │
 │ tripwire IVS │  multipart        │ parser → registro        │  cola SQLite │ (B5.3)     │
 └──────────────┘  Heartbeat/Start  │ → auditoría JSON Lines   │              └────────────┘
                                    └──────────────────────────┘
```

## Cómo funciona

1. Al arrancar lee el **serial** de la cámara (`GET /cgi-bin/magicBox.cgi?action=getSerialNo`,
   digest). Sin serial no abre el stream: reintenta con backoff.
2. Abre `GET /cgi-bin/eventManager.cgi?action=attach&codes=%5BCrossLineDetection%5D&heartbeat=<camera_heartbeat_seconds>`
   con digest. La cámara manda un multipart (`--myboundary`) con partes `text/plain`
   y `Content-Length`: `Heartbeat` cada N segundos y, por cada cruce, un par
   `Code=CrossLineDetection;action=Start;index=0;data={JSON}` / `action=Stop`.
3. El parser **entrega cada parte apenas llegan sus `Content-Length` bytes**, sin
   esperar la parte siguiente (la lectura usa `read1`, no bloques de 1024 bytes).
   Así un cruce llega a la auditoría y a la cola en el momento, no con el latido
   siguiente de la cámara.
4. Solo el `Start` genera registro (se envían **ambos sentidos**). `Stop` y
   `Heartbeat` no generan registro; el `Heartbeat` alimenta la vigilancia del stream.
5. Cada registro se serializa **una sola vez**: esa misma cadena va a la auditoría
   y a la cola, y el POST la reenvía siempre byte a byte igual.
6. Si la cámara calla más de `stream_idle_timeout` segundos, se corta y se
   reconecta con backoff **5 / 10 / 20 / 40 / 60 s** (tope 60), que vuelve a 5
   solo cuando llega el primer `Heartbeat` de la conexión nueva.
7. Cada `heartbeat_interval_minutes` emite un **latido propio** (registro
   `kind=heartbeat`), solo si el stream está sano. Una línea INFO por hora resume
   cruces, latidos de la cámara, reconexiones y pendientes de la cola.

## Instalación en Home Assistant

1. **Settings → Add-ons → Add-on Store → ⋮ → Repositories**.
2. Agregar `https://github.com/nupsterd/dahua-ivs-addon`.
3. Instalar **Dahua IVS (Portería Virtual)**. La imagen se construye en la Pi
   (no hay imagen prebuilt): tarda unos minutos la primera vez.
4. En **Configuration** completar al menos `camera_host`, `camera_user` y
   `camera_password` (activar *Show unused optional configuration options* para
   ver `backend_url` / `backend_secret`). Guardar y verificar desde el add-on SSH
   que el valor quedó: `ha apps info <slug> --raw-json | jq .data.options`.
5. En **Info**: activar **Start on boot** y **Watchdog**, y arrancar.
6. En **Log** tiene que aparecer `Serial de la cámara: …`, `Conectado al stream de
   eventos` y `Stream sano: primer Heartbeat de la cámara recibido.`

## Opciones

| Opción | Tipo (schema) | Default | Descripción |
|---|---|---|---|
| `camera_host` | `str` | — | IP (o `ip:puerto`) de la cámara. Obligatoria. |
| `camera_user` | `str` | `admin` | Usuario digest de la cámara. |
| `camera_password` | `password` | — | Clave de la cámara. Nunca se imprime en el log. |
| `camera_timezone` | `str` | `America/Bogota` | Zona del sitio: se usa para convertir el campo `UTC` de la cámara (que es hora local) a `device_ts`. |
| `camera_heartbeat_seconds` | `int(10,120)` | `30` | Latido que se le pide a la cámara en el `attach`. |
| `stream_idle_timeout` | `int(20,300)` | `75` | Segundos sin datos antes de reconectar. Debe ser `>= 2 x camera_heartbeat_seconds` (si no, el add-on sale con ERROR). |
| `heartbeat_interval_minutes` | `int(1,60)` | `15` | Cada cuánto se emite el latido propio. |
| `backend_url` | `str?` | vacío | **Endpoint COMPLETO** del backend (p. ej. `https://api.<dominio>/api/v1/eventos/dahua`). No se concatena ningún path. Vacío = fan-out apagado (solo auditoría, sin cola). |
| `backend_secret` | `password?` | vacío | Token que va en el header `X-PV-Dahua-Token`. Nunca se imprime. |
| `backend_timeout_seconds` | `int(1,30)` | `5` | Timeout de cada POST. |
| `outbox_max_records` | `int(1000,1000000)` | `100000` | Tope de la cola; al superarlo se descarta el registro **más viejo**. |
| `outbox_max_age_days` | `int(1,30)` | `7` | Registros más viejos que esto se descartan de la cola. |
| `audit_dir` | `str` | `/config` | Carpeta de la auditoría (`/config` = `/addon_configs/<slug>/`). |
| `audit_retention_days` | `int(1,365)` | `30` | Días de auditoría que se conservan. |
| `log_level` | `list(debug\|info\|warning)` | `info` | En `info` no se imprime cada cruce (van a `debug` y a la auditoría). |

## Auditoría local

`<audit_dir>/dahua_ivs_audit_YYYYMMDD.jsonl` (fecha local de la Pi), una línea
JSON por registro (cruces y latidos), flush por línea. Cada línea es
**exactamente** el cuerpo que va (o iría) en el POST. Los archivos con más de
`audit_retention_days` días se borran al arrancar y en cada cambio de día. Desde
el add-on SSH: `/addon_configs/<slug>/dahua_ivs_audit_$(date +%Y%m%d).jsonl`.

## Contrato del POST (para el backend, B5.3)

```
POST <backend_url>                      (tal cual; destino previsto: /api/v1/eventos/dahua)
Content-Type: application/json
X-PV-Dahua-Token: <backend_secret>      (se omite si backend_secret está vacío)

<un registro JSON por request>
```

### Registro de cruce (`kind = "crossing"`)

Ejemplo real (captura del 23-sep; serial e IP reemplazados por valores de ejemplo):

```json
{
  "device_kind": "camera_tripwire",
  "kind": "crossing",
  "device_serial": "9J00000EXAMPLE0",
  "device_ip": "192.0.2.21",
  "device_ts": "2026-09-23T08:15:33.420-05:00",
  "received_ts": "2026-09-23T08:15:34.120-05:00",
  "device_utc_raw": {"utc": 1790151333, "utcms": 420},
  "channel": 0,
  "rule_name": "salida 1",
  "rule_id": 4,
  "event_id": 10013,
  "direction": "LeftToRight",
  "object_id": 73,
  "object_type": "Human",
  "bbox": [2384, 3496, 3712, 8184],
  "center": [3048, 5840],
  "addon_version": "0.1.0-alpha"
}
```

(En el cable va compacto, en una línea, sin espacios: `{"device_kind":"camera_tripwire",...}`.)

| Campo | Origen | Nota |
|---|---|---|
| `device_kind` | constante | `camera_tripwire` |
| `kind` | constante | `crossing` |
| `device_serial` | `getSerialNo` al arrancar | identifica la cámara (el stream no trae MAC ni serial) |
| `device_ip` | `camera_host` | |
| `device_ts` | `data.UTC` + `data.UTCMS` | **`UTC` es la hora de pared LOCAL codificada como epoch** (1790151333 = 08:15:33 en Bogotá). Se interpreta como hora de pared + `camera_timezone`. ISO 8601 con offset y ms. `null` si la cámara no manda `UTC`. |
| `received_ts` | reloj de la Pi al parsear | ISO 8601 con offset y ms |
| `device_utc_raw` | `data.UTC`, `data.UTCMS` | valores originales sin tocar |
| `channel` | `index` de la línea `Code=…` | |
| `rule_name` | `data.Name` con `strip()` | la regla de la oficina se llama `"salida 1 "` (espacio final) |
| `rule_id`, `event_id`, `direction` | `data.RuleID`, `data.EventID`, `data.Direction` | `direction`: `LeftToRight` / `RightToLeft` (en la oficina `LeftToRight` = salida) |
| `object_id`, `object_type`, `bbox`, `center` | `data.Object.ObjectID/ObjectType/BoundingBox/Center` | `ObjectID` es un ID de seguimiento, no de persona |
| `addon_version` | constante | |

No se envía el `data` crudo: no trae imagen, rostro ni dato biométrico, pero se
recorta igual a lo que el backend necesita.

### Registro de latido (`kind = "heartbeat"`)

Solo se emite si el stream está sano (último `Heartbeat` de la cámara hace menos
de `stream_idle_timeout`). Si al cumplirse el intervalo el stream no está sano,
queda pendiente y sale en cuanto vuelva a estarlo.

```json
{
  "device_kind": "camera_tripwire",
  "kind": "heartbeat",
  "device_serial": "9J00000EXAMPLE0",
  "device_ip": "192.0.2.21",
  "received_ts": "2026-09-23T08:30:00.005-05:00",
  "crossings_since_last": 4,
  "camera_heartbeats_since_last": 30,
  "reconnects_since_last": 0,
  "outbox_pending": 0,
  "addon_version": "0.1.0-alpha"
}
```

`outbox_pending` es `null` cuando el fan-out está apagado (no hay cola).

### Llave de idempotencia

Para un cruce: **`device_serial` + `rule_id` + `object_id` + `device_ts`**. Dos
personas que cruzan en el mismo segundo tienen `object_id` distinto (y además
`device_ts` distinto por los milisegundos), así que nunca colisionan. Un reenvío
del mismo registro llega byte a byte igual. El latido no necesita deduplicación
(`received_ts` lo distingue).

### Respuestas del backend y comportamiento de la cola

| Respuesta | Acción del add-on |
|---|---|
| 2xx con `{"status": "received"}` o `{"status": "duplicate"}` | Borra el registro de la cola. |
| 2xx con `{"status": "device_unknown"}` | **ERROR DE CONFIGURACIÓN**: pausa el envío, no descarta nada, ERROR y reintento cada 10 min. |
| 2xx con otro `status` o sin JSON | ERROR DE CONFIGURACIÓN (igual que arriba). |
| 401, 403 | ERROR DE CONFIGURACIÓN (token). |
| 404, 405 | ERROR DE CONFIGURACIÓN (URL: tiene que ser el endpoint completo). |
| 400, 422 (y otros 4xx no listados) | Mueve ESE registro a la tabla `failed` (con código y motivo) y sigue con el siguiente. |
| 408, 429, 5xx, timeout, error de red | Reintenta el MISMO registro con backoff 5 s → 10 → 20 → 40 → 80 → 160 → 300 s (tope 5 min), conservando el orden. |

La cola es SQLite en `/data/outbox.sqlite` (WAL, `synchronous=FULL`), FIFO, un
solo hilo de envío. Sobrevive a reinicios del add-on y de la Pi. Límites
`outbox_max_records` y `outbox_max_age_days`: se descarta el **más viejo**, con
un WARNING como máximo cada 10 min. Solo existe si `backend_url` está
configurado.

## Límites conocidos

- **Si la Pi está apagada o el add-on caído, los cruces de ese lapso se pierden:**
  la cámara no guarda log local de cruces (regla creada con `LogEnable=false`,
  §5.9.625f) y el stream no reenvía lo pasado.
- La cola cubre caídas de **internet / backend**, no caídas de la Pi.
- El tripwire con montaje oblicuo no separa a dos personas pegadas al hombro
  (§5.9.627).
- Tripwire (IVS) y conteo (`NumberStat`) no pueden correr a la vez en la cámara.
- La auditoría crece con el tráfico; la retención la limita por días, no por tamaño.

## Troubleshooting

| Síntoma en el log | Causa probable | Qué hacer |
|---|---|---|
| `La cámara rechazó usuario/clave (401 …). Espera de 15 min` | Clave o usuario de la cámara cambiados | Corregir `camera_user`/`camera_password`, guardar y reiniciar el add-on (no esperar los 15 min). No insistir con claves de prueba: la cámara puede bloquear la cuenta. |
| `Stream sin datos durante N s` / `Stream cortado` en bucle | Cámara apagada, red, o la cámara dejó de mandar `Heartbeat` | Probar desde la Pi: `curl -s -m 8 --digest -u "admin:$DP" "http://<cam>/cgi-bin/magicBox.cgi?action=getSerialNo"`. Verificar que `stream_idle_timeout >= 2 x camera_heartbeat_seconds`. |
| No aparece `Stream sano: primer Heartbeat…` | La conexión abre pero no llegan latidos | Revisar la versión de firmware; probar el `attach` con `curl -N` (runbook §13.BE). |
| `ERROR DE CONFIGURACIÓN … device_unknown` | La cámara no está dada de alta en el backend con ese serial | Dar de alta el dispositivo con el `device_serial` que muestra el log. Nada se pierde: la cola espera. |
| `ERROR DE CONFIGURACIÓN … (HTTP 404)` | `backend_url` es la URL base | Poner el endpoint completo (`…/api/v1/eventos/dahua`). |
| `ERROR DE CONFIGURACIÓN … (HTTP 401)` | `backend_secret` incorrecto | Corregirlo y reiniciar. |
| `Reloj desfasado: device_ts=… received_ts=…` | NTP de la cámara apagado o `camera_timezone` mal puesta | Verificar NTP de la cámara (runbook §13.BE, paso 2) y la zona. Un desfase de exactamente 5 h indica zona mal interpretada. |
| `Cola llena o vencida … descartados` | Backend caído por días | Revisar el backend; los descartes son de los registros más viejos. |

## Desarrollo / tests

```bash
uv venv .venv && uv pip install pytest requests pyyaml tzdata
./.venv/bin/python -m pytest tests/ -q
```

Las capturas en `tests/fixtures/` son reales (oficina, 23-sep) y no contienen
datos personales. Módulos: `config`, `parser`, `records`, `outbox`, `audit`,
`stream`, `main` (en `dahua_ivs/`).
