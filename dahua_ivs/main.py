"""Entry point del add-on: ``python3 -m dahua_ivs.main``."""

from __future__ import annotations

import logging
import signal
import sys
import threading
from zoneinfo import ZoneInfo

from dahua_ivs import ADDON_VERSION
from dahua_ivs.audit import AuditWriter
from dahua_ivs.config import OPTIONS_PATH, Config
from dahua_ivs.outbox import OUTBOX_PATH, Outbox, OutboxSender
from dahua_ivs.stream import StreamRunner, Ticker

log = logging.getLogger("dahua_ivs")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger().setLevel(level.upper())
    # urllib3 en DEBUG imprime cada request; no aporta y ensucia el log.
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def load_config(path: str = OPTIONS_PATH) -> Config:
    try:
        return Config.from_options_json(path)
    except FileNotFoundError:
        logging.basicConfig(level="INFO")
        log.error("%s no existe. ¿Está corriendo dentro del add-on?", path)
        sys.exit(1)
    except (KeyError, TypeError, ValueError) as exc:
        logging.basicConfig(level="INFO")
        log.error("Configuración inválida en %s: %s", path, exc)
        sys.exit(1)


def startup(cfg: Config) -> None:
    """Valida y loguea la config. Sale con código 1 si es inválida."""
    setup_logging(cfg.log_level)
    errores = cfg.validate()
    for err in errores:
        log.error("Configuración inválida: %s", err)
    if errores:
        sys.exit(1)
    log.info("Dahua IVS (Portería Virtual) %s", ADDON_VERSION)
    for key, value in cfg.describe().items():
        log.info("  %s = %s", key, value)


def build(cfg: Config, outbox_path: str = OUTBOX_PATH) -> tuple[StreamRunner, Outbox | None, OutboxSender | None]:
    audit = AuditWriter(cfg.audit_dir, cfg.audit_retention_days)
    audit.purge()
    outbox: Outbox | None = None
    sender: OutboxSender | None = None
    if cfg.backend_enabled:
        outbox = Outbox(outbox_path, cfg.outbox_max_records, cfg.outbox_max_age_days)
        sender = OutboxSender(outbox, cfg.backend_url, cfg.backend_secret, cfg.backend_timeout_seconds)
        log.info(
            "Fan-out al backend ACTIVO: %s (%d registros pendientes en la cola).",
            cfg.backend_url,
            outbox.pending_count(),
        )
    else:
        log.info("Fan-out al backend APAGADO (backend_url vacío): solo auditoría local.")
    runner = StreamRunner(cfg, audit=audit, outbox=outbox, sender=sender, tz=ZoneInfo(cfg.camera_timezone))
    return runner, outbox, sender


def main() -> None:
    cfg = load_config()
    startup(cfg)
    stop = threading.Event()

    def on_signal(signum: int, _frame: object) -> None:
        log.info("Señal %s recibida: saliendo (la cola persiste en disco).", signum)
        stop.set()
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    runner, _outbox, sender = build(cfg)
    if sender is not None:
        threading.Thread(target=sender.run, args=(stop,), name="outbox-sender", daemon=True).start()
    threading.Thread(target=Ticker(runner).run, args=(stop,), name="ticker", daemon=True).start()
    try:
        runner.run(stop)
    finally:
        # La cola SQLite no se cierra acá: el hilo de envío puede estar usándola y
        # cada commit ya quedó en disco (WAL + synchronous=FULL).
        runner.audit.close()


if __name__ == "__main__":
    main()
