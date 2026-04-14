"""Tests for destination naming, PDF backup paths, and PDF safety heuristics."""

from __future__ import annotations

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


def test_unique_destination_same_file_not_infinite_loop(tmp_path: Path) -> None:
    f = tmp_path / "T.txt"
    f.write_text("x", encoding="utf-8")
    dest = odr.unique_destination(f, "T")
    assert dest == f


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
