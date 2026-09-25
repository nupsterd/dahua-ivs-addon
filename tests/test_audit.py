from __future__ import annotations

from datetime import datetime, timedelta

from dahua_ivs.audit import AuditWriter
from tests.conftest import BOGOTA, FakeClock


def test_archivo_diario_y_rotacion(tmp_path):
    clock = FakeClock(datetime(2026, 9, 23, 23, 59, 59, tzinfo=BOGOTA))
    w = AuditWriter(tmp_path, 30, now=clock.now)
    w.write('{"n":1}')
    clock.advance(2)  # pasa la medianoche local
    w.write('{"n":2}')
    w.close()
    assert (tmp_path / "dahua_ivs_audit_20260923.jsonl").read_text() == '{"n":1}\n'
    assert (tmp_path / "dahua_ivs_audit_20260924.jsonl").read_text() == '{"n":2}\n'


def test_flush_por_linea(tmp_path):
    clock = FakeClock()
    w = AuditWriter(tmp_path, 30, now=clock.now)
    w.write('{"n":1}')
    # Sin cerrar: ya está en disco.
    assert (tmp_path / "dahua_ivs_audit_20260923.jsonl").read_text() == '{"n":1}\n'
    w.close()


def test_retencion_borra_lo_viejo_y_respeta_lo_ajeno(tmp_path):
    hoy = datetime(2026, 9, 23, 10, 0, tzinfo=BOGOTA)
    for dias in (0, 29, 30, 31, 90):
        d = hoy - timedelta(days=dias)
        (tmp_path / f"dahua_ivs_audit_{d:%Y%m%d}.jsonl").write_text("x\n")
    (tmp_path / "face_audit.log").write_text("ajeno\n")
    (tmp_path / "dahua_ivs_audit_basura.jsonl").write_text("x\n")
    w = AuditWriter(tmp_path, 30, now=lambda: hoy)
    borrados = sorted(p.name for p in w.purge())
    assert borrados == ["dahua_ivs_audit_20260625.jsonl", "dahua_ivs_audit_20260823.jsonl"]
    restantes = sorted(p.name for p in tmp_path.iterdir())
    assert restantes == [
        "dahua_ivs_audit_20260824.jsonl",
        "dahua_ivs_audit_20260825.jsonl",
        "dahua_ivs_audit_20260923.jsonl",
        "dahua_ivs_audit_basura.jsonl",
        "face_audit.log",
    ]


def test_error_de_disco_no_corta(tmp_path, caplog):
    archivo = tmp_path / "no-es-dir"
    archivo.write_text("")
    w = AuditWriter(archivo, 30, now=FakeClock().now)
    w.write('{"n":1}')  # no levanta
    assert any("auditoría" in r.getMessage() for r in caplog.records)
