"""Sanity checks that autouse mocks in conftest.py cover the Ollama call stack."""

from __future__ import annotations

import ollama_document_renamer as odr


def test_analyze_text_with_ollama_runs_against_default_http_mock() -> None:
    result = odr.analyze_text_with_ollama(
        text="Sample document body " * 10,
        text_model="llama3:latest",
        ollama_url="http://127.0.0.1:11434",
        filename="sample.txt",
        source_kind="text",
        backend="http",
    )
    assert "Mock" in result.title
    assert result.summary
    assert isinstance(result.metadata, dict)


def test_ollama_chat_auto_succeeds_via_http_mock() -> None:
    result = odr.ollama_chat(
        {
            "model": "m",
            "messages": [],
            "prompt": "p",
        },
        "http://127.0.0.1:11434",
        "auto",
    )
    assert "Mock" in result["title"]
