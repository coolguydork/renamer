"""Tests for JSON extraction, Ollama response parsing, URLs, and payloads."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

import ollama_document_renamer as odr


def test_extract_json_object_success() -> None:
    s = 'prefix {"a": 1} suffix'
    assert json.loads(odr.extract_json_object(s)) == {"a": 1}


def test_extract_json_object_failure() -> None:
    with pytest.raises(json.JSONDecodeError):
        odr.extract_json_object("no braces here")


def test_parse_lenient_json_response_strict_json() -> None:
    raw = '{"title": "T", "summary": "S", "metadata": {}}'
    d = odr.parse_lenient_json_response(raw)
    assert d["title"] == "T"
    assert d["summary"] == "S"


def test_parse_lenient_json_response_lenient_quotes() -> None:
    raw = r'noise {"title": "My \"Title\"", "summary": "Line\nTwo", "metadata": {}} tail'
    d = odr.parse_lenient_json_response(raw)
    assert "Title" in d["title"]


def test_parse_lenient_json_response_missing_title_raises() -> None:
    raw = '{"summary": "only",}'  # invalid JSON so strict parse fails; lenient regex needs title
    with pytest.raises(RuntimeError, match="title/summary"):
        odr.parse_lenient_json_response(raw)


def test_clean_model_string() -> None:
    assert odr.clean_model_string('  a\\nb  ') == "a b"


def test_ollama_api_url() -> None:
    assert odr.ollama_api_url("http://127.0.0.1:11434", "/api/tags") == (
        "http://127.0.0.1:11434/api/tags"
    )


def test_ollama_api_url_invalid() -> None:
    with pytest.raises(RuntimeError, match="invalid"):
        odr.ollama_api_url("not-a-url", "/x")


def test_normalize_model_name() -> None:
    assert odr.normalize_model_name("Llama3") == "llama3:latest"
    assert odr.normalize_model_name("x:y") == "x:y"


def test_build_chat_payload_structure() -> None:
    p = odr.build_chat_payload(
        {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    )
    assert p["model"] == "m"
    assert p["stream"] is False
    assert "format" in p
    assert p["messages"][0]["content"] == "hi"


def test_parse_ollama_response_chat_mode() -> None:
    inner = {"title": "T", "summary": "S", "metadata": {}}
    raw = json.dumps({"message": {"content": json.dumps(inner)}})
    out = odr.parse_ollama_response(raw, "chat")
    assert out["title"] == "T"
    assert "document_type" in out["metadata"]


def test_parse_ollama_response_generate_mode() -> None:
    inner = {"title": "T", "summary": "S", "metadata": {}}
    raw = json.dumps({"response": json.dumps(inner)})
    out = odr.parse_ollama_response(raw, "generate")
    assert out["summary"] == "S"


def test_parse_ollama_response_bad_json_raises() -> None:
    with pytest.raises(RuntimeError, match="unexpected"):
        odr.parse_ollama_response("{{{", "chat")


def test_parse_ollama_response_incomplete_inner_raises() -> None:
    raw = json.dumps({"message": {"content": json.dumps({"title": "", "summary": "S"})}})
    with pytest.raises(RuntimeError, match="incomplete"):
        odr.parse_ollama_response(raw, "chat")


def test_ensure_model_available_when_list_empty_noop() -> None:
    with patch.object(odr, "fetch_available_models", return_value=[]):
        odr.ensure_model_available("any", "http://127.0.0.1:11434")


def test_ensure_model_available_exact_match() -> None:
    with patch.object(odr, "fetch_available_models", return_value=["llama3:latest"]):
        odr.ensure_model_available("llama3:latest", "http://127.0.0.1:11434")


def test_ensure_model_available_suggests_normalized() -> None:
    with patch.object(odr, "fetch_available_models", return_value=["Llama3:latest"]):
        with pytest.raises(RuntimeError, match="Llama3:latest"):
            odr.ensure_model_available("llama3", "http://127.0.0.1:11434")


def test_ensure_model_available_not_found() -> None:
    with patch.object(odr, "fetch_available_models", return_value=["a", "b"]):
        with pytest.raises(RuntimeError, match="not installed"):
            odr.ensure_model_available("missing", "http://127.0.0.1:11434")


def test_extract_metadata_object() -> None:
    assert odr.extract_metadata_object({"metadata": {"x": 1}}) == {"x": 1}
    assert odr.extract_metadata_object({}) == {}
    assert odr.extract_metadata_object({"metadata": "bad"}) == {}
