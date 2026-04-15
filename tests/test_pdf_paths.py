"""Tests for destination naming, PDF backups, and PDF safety heuristics."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import ollama_document_renamer as odr


def test_unique_destination_no_collision(tmp_path: Path) -> None:
    f = tmp_path / "old.txt"
    f.write_text("x", encoding="utf-8")
    dest = odr.unique_destination(f, "New Title")
    assert dest == tmp_path / "New Title.txt"


def test_unique_destination_appends_counter(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("1", encoding="utf-8")
    (tmp_path / "T.txt").write_text("2", encoding="utf-8")
    dest = odr.unique_destination(f, "T")
    assert dest.name == "T 2.txt"


def test_unique_destination_same_file_no_loop(tmp_path: Path) -> None:
    f = tmp_path / "T.txt"
    f.write_text("x", encoding="utf-8")
    dest = odr.unique_destination(f, "T")
    assert dest == f


def test_unique_destination_strips_title_pdf_suffix(tmp_path: Path) -> None:
    f = tmp_path / "old.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    dest = odr.unique_destination(f, "March 2025 Pay Stub Acme Corp.pdf")
    assert dest.name == "March 2025 Pay Stub Acme Corp.pdf"
    assert not dest.name.endswith(".pdf.pdf")


def test_unique_destination_strips_double_pdf_suffix(tmp_path: Path) -> None:
    f = tmp_path / "old.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    dest = odr.unique_destination(f, "Report.pdf.pdf")
    assert dest.name == "Report.pdf"


def test_unique_destination_extension_case_insensitive(tmp_path: Path) -> None:
    f = tmp_path / "old.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    dest = odr.unique_destination(f, "Summary.PDF")
    assert dest.name == "Summary.pdf"


def test_build_pdf_backup_path_default_suffix() -> None:
    p = Path("/tmp/doc.pdf")
    b = odr.build_pdf_backup_path(p, ".metadata-backup.pdf")
    assert b.name == "doc.metadata-backup.pdf"


def test_build_pdf_backup_path_non_pdf_suffix_gets_pdf_extension() -> None:
    p = Path("/tmp/doc.pdf")
    b = odr.build_pdf_backup_path(p, "-bak")
    assert b.name == "doc-bak.pdf"


def test_build_pdf_backup_path_empty_suffix_uses_default() -> None:
    p = Path("/a/x.pdf")
    b = odr.build_pdf_backup_path(p, "")
    assert "backup" in b.name.lower() or b.suffix == ".pdf"


def test_inspect_pdf_safety_plain() -> None:
    sample = b"%PDF-1.4\n1 0 obj<<>>endobj\n%%EOF"
    path = Path(__file__).with_name("_tmp_plain.pdf")
    try:
        path.write_bytes(sample)
        state = odr.inspect_pdf_safety(path)
        assert state["is_encrypted"] is False
        assert state["has_digital_signature"] is False
    finally:
        path.unlink(missing_ok=True)


def test_inspect_pdf_safety_encrypted_flag() -> None:
    sample = b"%PDF-1.4\n/Encrypt\n"
    path = Path(__file__).with_name("_tmp_enc.pdf")
    try:
        path.write_bytes(sample)
        assert odr.inspect_pdf_safety(path)["is_encrypted"] is True
    finally:
        path.unlink(missing_ok=True)


def test_inspect_pdf_safety_signature_byterange() -> None:
    sample = b"%PDF-1.4\n/ByteRange [0 1 2 3]\n"
    path = Path(__file__).with_name("_tmp_sig.pdf")
    try:
        path.write_bytes(sample)
        assert odr.inspect_pdf_safety(path)["has_digital_signature"] is True
    finally:
        path.unlink(missing_ok=True)


def test_inspect_pdf_safety_missing_file_raises(tmp_path: Path) -> None:
    p = tmp_path / "nope.pdf"
    with pytest.raises(RuntimeError, match="inspect"):
        odr.inspect_pdf_safety(p)


def test_pdf_structure_broken_when_exiftool_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"x")
    monkeypatch.setattr(
        odr.shutil,
        "which",
        lambda name: "/fake/exiftool" if name == "exiftool" else None,
    )

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(cmd, 1, "", "bad xref")

    monkeypatch.setattr(odr.subprocess, "run", fake_run)
    assert odr.pdf_structure_likely_broken_for_exiftool(pdf) is True


def test_maybe_repair_skips_encrypted(tmp_path: Path) -> None:
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4\n/Encrypt\n")
    msg = odr.maybe_repair_pdf_if_needed(pdf, dry_run=False, repair_backup_suffix=".bak.pdf")
    assert msg == "skipped (encrypted PDF)"


def test_maybe_repair_dry_run_when_structure_broken(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    monkeypatch.setattr(odr, "pdf_structure_likely_broken_for_exiftool", lambda p: True)
    msg = odr.maybe_repair_pdf_if_needed(pdf, dry_run=True, repair_backup_suffix=".bak.pdf")
    assert msg and "would repair" in msg


def test_maybe_repair_noop_for_healthy_pdf(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    monkeypatch.setattr(odr, "pdf_structure_likely_broken_for_exiftool", lambda p: False)
    assert odr.maybe_repair_pdf_if_needed(pdf, dry_run=False, repair_backup_suffix=".bak.pdf") is None
