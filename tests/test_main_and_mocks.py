"""Tests for main() exit codes, process_file, and Ollama/subprocess edge cases."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from threading import Lock
from unittest.mock import patch

import pytest

import ollama_document_renamer as odr


def test_main_directory_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", "/nonexistent_dir_xyz_12345"])
    assert odr.main() == 1


def test_main_invalid_exclude_regex(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", str(tmp_path), "--exclude-regex", "("])
    assert odr.main() == 1


def test_main_repair_pdf_requires_qpdf(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello world " * 20, encoding="utf-8")

    def which(name: str) -> str | None:
        return None if name == "qpdf" else "/usr/bin/true"

    monkeypatch.setattr(odr.shutil, "which", which)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", str(tmp_path), "--repair-pdf-if-needed", "--max-files", "1"],
    )
    assert odr.main() == 1


def test_main_workers_zero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("x", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", str(tmp_path), "--dry-run", "--workers", "0"],
    )
    assert odr.main() == 1


def test_main_audit_log_exists_without_overwrite(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("x", encoding="utf-8")
    log = tmp_path / "audit.jsonl"
    log.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", str(tmp_path), "--audit-log", str(log)],
    )
    assert odr.main() == 1


def test_main_no_files_empty_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", str(tmp_path), "--dry-run"])
    assert odr.main() == 0


def test_main_resume_skips_paths_recorded_in_audit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    a = docs / "a.txt"
    a.write_text("hello world " * 20, encoding="utf-8")
    log = tmp_path / "audit.jsonl"
    log.write_text(
        json.dumps({"renamed_path": str(a.resolve())}) + "\n",
        encoding="utf-8",
    )
    calls: list[Path] = []

    def fake_process_file(file_path: Path, **kwargs: object) -> odr.ProcessOutcome:
        calls.append(file_path)
        return odr.ProcessOutcome(
            file_path,
            file_path,
            odr.AnalysisResult("X", "S", "text", {}),
        )

    monkeypatch.setattr(odr, "process_file", fake_process_file)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", str(docs), "--dry-run", "--resume", "--audit-log", str(log)],
    )
    assert odr.main() == 0
    assert calls == []


def test_main_max_files_limits_processing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "a.txt").write_text("x", encoding="utf-8")
    (tmp_path / "b.txt").write_text("y", encoding="utf-8")
    seen: list[str] = []

    def fake_process_file(file_path: Path, **kwargs: object) -> odr.ProcessOutcome:
        seen.append(file_path.name)
        return odr.ProcessOutcome(
            file_path,
            file_path.with_name("R.txt"),
            odr.AnalysisResult("R", "S", "text", {}),
        )

    monkeypatch.setattr(odr, "process_file", fake_process_file)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", str(tmp_path), "--dry-run", "--max-files", "1"],
    )
    assert odr.main() == 0
    assert len(seen) == 1


def test_main_dry_run_invokes_process_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "doc.txt").write_text("hello world " * 20, encoding="utf-8")

    def fake_process_file(file_path: Path, **kwargs: object) -> odr.ProcessOutcome:
        return odr.ProcessOutcome(
            original_path=file_path,
            renamed_path=file_path.with_name("Renamed.txt"),
            result=odr.AnalysisResult(
                title="Renamed",
                summary="A short summary.",
                source_kind="text",
                metadata={"document_type": "Note", "keywords": ["k"]},
            ),
        )

    monkeypatch.setattr(odr, "process_file", fake_process_file)
    monkeypatch.setattr(sys, "argv", ["prog", str(tmp_path), "--dry-run"])
    assert odr.main() == 0
    out = capsys.readouterr().out
    assert "WOULD RENAME" in out
    assert "Renamed" in out


def test_process_file_skips_empty_title_after_sanitize(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    src = tmp_path / "x.txt"
    src.write_text("data", encoding="utf-8")
    monkeypatch.setattr(
        odr,
        "analyze_file",
        lambda **kw: odr.AnalysisResult(title="///", summary="s", source_kind="text", metadata={}),
    )
    lock = Lock()
    outcome = odr.process_file(
        file_path=src,
        text_model="m",
        vision_model="m",
        ollama_url="http://127.0.0.1:11434",
        backend="http",
        dry_run=False,
        write_spotlight_comment_flag=False,
        write_pdf_metadata_flag=False,
        pdf_backup_suffix=".bak.pdf",
        validate_pdf_after_write=False,
        delete_pdf_backup_on_success=False,
        audit_handle=None,
        write_lock=lock,
    )
    assert outcome.skipped_reason == "model returned an empty title"
    assert src.exists()


def test_process_file_catches_analyze_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    src = tmp_path / "x.txt"
    src.write_text("data", encoding="utf-8")

    def boom(**kwargs: object) -> None:
        raise RuntimeError("network down")

    monkeypatch.setattr(odr, "analyze_file", boom)
    lock = Lock()
    outcome = odr.process_file(
        file_path=src,
        text_model="m",
        vision_model="m",
        ollama_url="http://127.0.0.1:11434",
        backend="http",
        dry_run=False,
        write_spotlight_comment_flag=False,
        write_pdf_metadata_flag=False,
        pdf_backup_suffix=".bak.pdf",
        validate_pdf_after_write=False,
        delete_pdf_backup_on_success=False,
        audit_handle=None,
        write_lock=lock,
    )
    assert "network down" in (outcome.skipped_reason or "")


def test_process_file_writes_audit_line(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    src = tmp_path / "old_name.txt"
    src.write_text("data", encoding="utf-8")
    monkeypatch.setattr(
        odr,
        "analyze_file",
        lambda **kw: odr.AnalysisResult(title="Final", summary="sum", source_kind="text", metadata={}),
    )
    log_path = tmp_path / "out.jsonl"
    lock = Lock()
    with log_path.open("w", encoding="utf-8") as handle:
        odr.process_file(
            file_path=src,
            text_model="m",
            vision_model="m",
            ollama_url="http://127.0.0.1:11434",
            backend="http",
            dry_run=False,
            write_spotlight_comment_flag=False,
            write_pdf_metadata_flag=False,
            pdf_backup_suffix=".bak.pdf",
            validate_pdf_after_write=False,
            delete_pdf_backup_on_success=False,
            audit_handle=handle,
            write_lock=lock,
        )
    assert not src.exists()
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row.get("status") == "ok"
    assert row["title"] == "Final"
    assert "Final.txt" in row["renamed_path"]


def test_process_file_writes_skipped_audit_line(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    src = tmp_path / "bad.txt"
    src.write_text("data", encoding="utf-8")

    def boom(**kwargs: object) -> None:
        raise RuntimeError("model unavailable")

    monkeypatch.setattr(odr, "analyze_file", boom)
    log_path = tmp_path / "out.jsonl"
    lock = Lock()
    with log_path.open("w", encoding="utf-8") as handle:
        odr.process_file(
            file_path=src,
            text_model="m",
            vision_model="m",
            ollama_url="http://127.0.0.1:11434",
            backend="http",
            dry_run=False,
            write_spotlight_comment_flag=False,
            write_pdf_metadata_flag=False,
            pdf_backup_suffix=".bak.pdf",
            validate_pdf_after_write=False,
            delete_pdf_backup_on_success=False,
            audit_handle=handle,
            write_lock=lock,
        )
    row = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert row["status"] == "skipped"
    assert row["skipped_reason"] == "model unavailable"
    assert "renamed_path" not in row


def test_report_outcome_skip_and_success(capsys: pytest.CaptureFixture[str]) -> None:
    odr.report_outcome(
        odr.ProcessOutcome(
            Path("/x/a.txt"),
            None,
            None,
            skipped_reason="bad",
        ),
        dry_run=False,
    )
    err = capsys.readouterr().err
    assert "SKIP" in err

    capsys.readouterr()
    odr.report_outcome(
        odr.ProcessOutcome(
            Path("/x/b.txt"),
            Path("/x/B.txt"),
            odr.AnalysisResult("B", "s", "text", {}),
        ),
        dry_run=True,
    )
    assert "WOULD RENAME" in capsys.readouterr().out


def test_ollama_chat_http_propagates_when_backend_http() -> None:
    with (
        patch.object(odr, "ollama_chat_http", side_effect=RuntimeError("fail")),
        pytest.raises(RuntimeError, match="fail"),
    ):
        odr.ollama_chat({}, "http://127.0.0.1:11434", "http")


def test_ollama_chat_auto_falls_back_to_cli() -> None:
    inner = {"title": "T", "summary": "S", "metadata": {}}
    with (
        patch.object(odr, "ollama_chat_http", side_effect=RuntimeError("down")),
        patch.object(odr, "ollama_chat_cli", return_value=inner),
    ):
        out = odr.ollama_chat({"model": "m"}, "http://127.0.0.1:11434", "auto")
        assert out["title"] == "T"


def test_ollama_chat_cli_rejects_images() -> None:
    payload = {"model": "m", "prompt": "p", "images": ["x"]}
    with pytest.raises(RuntimeError, match="CLI fallback"):
        odr.ollama_chat_cli(payload)


def test_ollama_chat_unsupported_backend() -> None:
    with pytest.raises(RuntimeError, match="unsupported"):
        odr.ollama_chat({}, "http://127.0.0.1:11434", "nope")  # type: ignore[arg-type]


def test_write_spotlight_comment_noop_for_empty() -> None:
    odr.write_spotlight_comment(Path("/tmp/x"), "")


def test_write_spotlight_comment_requires_xattr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(odr.shutil, "which", lambda name: None if name == "xattr" else "/bin/true")
    with pytest.raises(RuntimeError, match="xattr"):
        odr.write_spotlight_comment(Path("/tmp/x"), "hi")


def test_write_pdf_metadata_requires_exiftool(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(odr.shutil, "which", lambda name: None if name == "exiftool" else "/usr/bin/true")
    pdf = tmp_path / "f.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    r = odr.AnalysisResult("t", "s", "text", {})
    with pytest.raises(RuntimeError, match="exiftool"):
        odr.write_pdf_metadata_conservatively(
            pdf, r, ".bak.pdf", False, False
        )


def test_extract_plain_text_supported_extension(tmp_path: Path) -> None:
    f = tmp_path / "n.txt"
    f.write_text("hello", encoding="utf-8")
    assert "hello" in odr.extract_plain_text(f)


def test_extract_plain_text_unsupported_extension(tmp_path: Path) -> None:
    f = tmp_path / "bin.exe"
    f.write_bytes(b"\x00\x01")
    assert odr.extract_plain_text(f) == ""


def test_analyze_file_unsupported_extension(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    f = tmp_path / "weird.xyz"
    f.write_text("x", encoding="utf-8")
    monkeypatch.setattr(odr, "extract_text", lambda _path: "")
    with pytest.raises(RuntimeError, match="no supported extraction"):
        odr.analyze_file(f, "m", "m", "http://127.0.0.1:11434", "http")


def test_render_pdf_preview_requires_qlmanage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(odr.shutil, "which", lambda name: None)
    p = tmp_path / "a.pdf"
    p.write_bytes(b"%PDF-1.4\n")
    with pytest.raises(RuntimeError, match="qlmanage"):
        odr.render_pdf_preview(p)


def test_render_pdf_preview_page_gt_one_requires_swift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(odr.shutil, "which", lambda name: None if name == "swift" else "/bin/qlmanage")
    p = tmp_path / "a.pdf"
    p.write_bytes(b"%PDF-1.4\n")
    with pytest.raises(RuntimeError, match="swift"):
        odr.render_pdf_preview(p, page=2)


def test_render_pdf_preview_page_gt_one_runs_swift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert cmd[0] == "swift"
        assert "macos_pdf_page_render.swift" in cmd[1]
        assert cmd[3] == "2"
        Path(cmd[4]).write_bytes(b"fakepng")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(odr.subprocess, "run", fake_run)
    monkeypatch.setattr(odr.shutil, "which", lambda name: "/swift" if name == "swift" else None)

    out = odr.render_pdf_preview(pdf, page=2)
    try:
        assert out.read_bytes() == b"fakepng"
    finally:
        out.unlink(missing_ok=True)


def test_main_rejects_pdf_preview_page_zero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("x", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", str(tmp_path), "--pdf-preview-page", "0", "--dry-run"],
    )
    assert odr.main() == 1


def test_drain_executor_futures_handles_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from concurrent.futures import Future

    fut: Future = Future()
    fut.cancel()
    shutdown = __import__("threading").Event()
    interrupted = odr._drain_executor_futures([fut], shutdown, dry_run=True, pbar=None)
    assert interrupted is False


def test_install_and_restore_signal_handlers() -> None:
    import signal

    shutdown = __import__("threading").Event()
    prior_int = signal.getsignal(signal.SIGINT)
    old_i, old_t = odr._install_graceful_interrupt_handlers(shutdown)
    try:
        assert signal.getsignal(signal.SIGINT) != prior_int
    finally:
        odr._restore_graceful_interrupt_handlers(old_i, old_t)
    assert signal.getsignal(signal.SIGINT) == prior_int
