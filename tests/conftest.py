"""Isolate tests from Ollama, the network, and optional macOS CLI tools.

Autouse fixtures here make the suite pass in CI and on machines without
Ollama, exiftool, qlmanage, etc. Tests that need specific failure modes
still override these with monkeypatch or unittest.mock.patch (applied
after the autouse hook, so their stubs win for the duration of the test).
"""

from __future__ import annotations

import os
import shutil as _real_shutil

import pytest

import ollama_document_renamer as odr

# Binaries the renamer shells out to; treat as absent unless a test replaces
# odr.shutil.which (or the relevant function) with its own stub.
_OFFLINE_BINARIES = frozenset({
    "swift",
    "textutil",
    "mdls",
    "qlmanage",
    "ollama",
    "xattr",
    "exiftool",
})


def _which_hide_external_tools(
    cmd: str,
    mode: int = os.F_OK | os.X_OK,
    path: str | None = None,
) -> str | None:
    if cmd in _OFFLINE_BINARIES:
        return None
    return _real_shutil.which(cmd, mode, path)


def _fake_ollama_chat_http(payload: dict, ollama_url: str) -> dict:  # noqa: ARG001
    """Return a response shaped like parse_ollama_response without calling the API."""
    return {
        "title": "Mock document title",
        "summary": "A concise mock summary for automated tests.",
        "metadata": odr.normalize_metadata({}),
    }


def _fake_ollama_chat_cli(payload: dict) -> dict:
    if payload.get("images"):
        raise RuntimeError(
            "CLI fallback does not support image inputs. Use the HTTP backend with a vision-capable Ollama server."
        )
    return {
        "title": "Mock CLI document title",
        "summary": "A concise mock CLI summary for automated tests.",
        "metadata": odr.normalize_metadata({}),
    }


def _fake_fetch_available_models(ollama_url: str) -> list[str]:  # noqa: ARG001
    """Avoid /api/tags HTTP; ensure_model_available treats this as 'unknown' and skips checks."""
    return []


def _block_urlopen(*_args: object, **_kwargs: object) -> None:
    raise RuntimeError(
        "Unexpected urllib.request.urlopen during tests; mock Ollama HTTP in this test."
    )


@pytest.fixture(autouse=True)
def _isolate_external_services(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(odr, "fetch_available_models", _fake_fetch_available_models)
    monkeypatch.setattr(odr, "ollama_chat_http", _fake_ollama_chat_http)
    monkeypatch.setattr(odr, "ollama_chat_cli", _fake_ollama_chat_cli)
    monkeypatch.setattr(odr.shutil, "which", _which_hide_external_tools)
    monkeypatch.setattr(odr.urllib.request, "urlopen", _block_urlopen)
