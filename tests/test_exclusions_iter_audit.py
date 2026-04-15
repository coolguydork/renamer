"""Tests for path exclusion, directory walking, and audit log resume helpers."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import ollama_document_renamer as odr


def test_path_matches_exclude_glob_basename_only() -> None:
    assert odr._path_matches_exclude_glob("a/b/foo.tmp", "foo.tmp", "*.tmp")
    assert not odr._path_matches_exclude_glob("a/b/foo.txt", "foo.txt", "*.tmp")


def test_path_matches_exclude_glob_full_relative_path() -> None:
    assert odr._path_matches_exclude_glob("vendor/pkg/x", "x", "vendor/*")
    assert not odr._path_matches_exclude_glob("other/pkg/x", "x", "vendor/*")


def test_is_excluded_rel_path_combines_glob_and_regex() -> None:
    globs = ("*.log",)
    regexes = (re.compile(r"secret/"),)
    assert odr._is_excluded_rel_path("app.log", "app.log", globs, regexes)
    assert odr._is_excluded_rel_path("secret/file.txt", "file.txt", (), regexes)
    assert not odr._is_excluded_rel_path("ok/file.txt", "file.txt", globs, regexes)


def test_iter_files_skips_hidden_by_default(tmp_path: Path) -> None:
    (tmp_path / "visible.txt").write_text("hi", encoding="utf-8")
    (tmp_path / ".hidden").write_text("x", encoding="utf-8")
    nested = tmp_path / "d"
    nested.mkdir()
    (nested / ".nested").write_text("y", encoding="utf-8")
    files = list(odr.iter_files(tmp_path, include_hidden=False))
    assert {p.name for p in files} == {"visible.txt"}


def test_iter_files_include_hidden(tmp_path: Path) -> None:
    (tmp_path / ".vis").write_text("a", encoding="utf-8")
    files = list(odr.iter_files(tmp_path, include_hidden=True))
    assert any(p.name == ".vis" for p in files)


def test_iter_files_skips_ds_store_case_insensitive(tmp_path: Path) -> None:
    (tmp_path / ".DS_Store").write_bytes(b"x")
    (tmp_path / "keep.txt").write_text("ok", encoding="utf-8")
    files = list(odr.iter_files(tmp_path, include_hidden=True))
    assert {p.name for p in files} == {"keep.txt"}


def test_iter_files_exclude_glob_skips_directory_descent(tmp_path: Path) -> None:
    skip = tmp_path / "node_modules"
    skip.mkdir()
    (skip / "bad.js").write_text("//", encoding="utf-8")
    (tmp_path / "ok.txt").write_text("ok", encoding="utf-8")
    files = list(odr.iter_files(tmp_path, False, exclude_globs=("node_modules",)))
    assert len(files) == 1
    assert files[0].name == "ok.txt"


def test_iter_files_exclude_git_repos_skips_nested_repo(tmp_path: Path) -> None:
    nested = tmp_path / "subrepo"
    nested.mkdir()
    (nested / ".git").mkdir()
    (nested / "in-repo.txt").write_text("x", encoding="utf-8")
    (tmp_path / "root.txt").write_text("y", encoding="utf-8")
    files = list(odr.iter_files(tmp_path, False, exclude_git_repos=True))
    assert {p.resolve() for p in files} == {(tmp_path / "root.txt").resolve()}


def test_load_completed_renamed_paths_missing_file(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    assert odr.load_completed_renamed_paths(root / "nope.jsonl", root) == set()


def test_load_completed_renamed_paths_filters_and_skips_bad_lines(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    inside = root / "done.txt"
    inside.write_text("x", encoding="utf-8")
    outside = tmp_path.parent / "outside_audit.txt"
    log = root / "audit.jsonl"
    log.write_text(
        "\n".join(
            [
                "not json",
                json.dumps({"renamed_path": str(inside)}),
                json.dumps({"renamed_path": str(outside)}),
                json.dumps({"foo": 1}),
                "",
            ]
        ),
        encoding="utf-8",
    )
    completed = odr.load_completed_renamed_paths(log, root)
    assert completed == {inside.resolve()}


def test_load_completed_renamed_paths_ignores_skipped_status(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    would_retry = root / "retry_me.pdf"
    would_retry.write_text("x", encoding="utf-8")
    log = root / "audit.jsonl"
    log.write_text(
        json.dumps(
            {
                "status": "skipped",
                "original_path": str(would_retry),
                "skipped_reason": "bad xref",
                "renamed_path": str(would_retry),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    assert odr.load_completed_renamed_paths(log, root) == set()
