"""Tests for title/summary sanitization, metadata normalization, and comment building."""

from __future__ import annotations

import ollama_document_renamer as odr


def test_meaningful_text_threshold() -> None:
    assert not odr.meaningful_text("")
    assert not odr.meaningful_text("short " * 5)
    long_enough = "word " * 20
    assert odr.meaningful_text(long_enough)


def test_truncate_text_collapses_whitespace() -> None:
    t = "a\n\nb\t\tc" + " x" * 8000
    out = odr.truncate_text(t)
    assert "\n" not in out
    assert len(out) <= odr.MAX_TEXT_CHARS


def test_sanitize_title_strips_illegal_chars_and_truncates() -> None:
    assert odr.sanitize_title('  hello/world:test  ') == "hello world test"
    long = "a" * 200
    assert len(odr.sanitize_title(long)) <= odr.TITLE_MAX_LENGTH


def test_sanitize_title_empty_after_strip() -> None:
    assert odr.sanitize_title('...///') == ""


def test_clean_summary() -> None:
    s = "too   many\nspaces" + " x" * 500
    out = odr.clean_summary(s)
    assert len(out) <= 400
    assert "  " not in out


def test_normalize_metadata_coerces_and_limits() -> None:
    raw = {
        "document_type": "  Report\n",
        "people": ["  A ", "A", 99, "b"],
        "organizations": "Acme; Corp",
        "bogus": "ignored",
    }
    n = odr.normalize_metadata(raw)
    assert n["document_type"] == "Report"
    assert n["people"] == ["A", "b"]
    assert "Acme" in str(n["organizations"])


def test_normalize_metadata_non_dict() -> None:
    n = odr.normalize_metadata(None)
    assert n["document_type"] == ""
    assert n["people"] == []


def test_normalize_string_list_from_string() -> None:
    assert odr.normalize_string_list("a; b, c", max_items=10) == ["a", "b", "c"]


def test_format_metadata_preview() -> None:
    m: dict[str, object] = {
        "document_type": "Invoice",
        "keywords": ["k1", "k2", "k3", "k4", "k5", "k6"],
        "people": ["Alice", "Bob", "Carol", "Dan"],
    }
    prev = odr.format_metadata_preview(m)
    assert "Invoice" in prev
    assert "k6" not in prev


def test_join_metadata_values() -> None:
    assert odr.join_metadata_values("K", []) == ""
    assert odr.join_metadata_values("K", "nope") == ""
    assert "a" in odr.join_metadata_values("K", ["a", "b"])


def test_build_spotlight_comment_length_cap() -> None:
    meta = {
        "keywords": ["x"] * 20,
        "people": ["p"] * 20,
        "document_type": "T",
        "language": "en",
    }
    r = odr.AnalysisResult(
        title="t",
        summary="s" * 2000,
        source_kind="text",
        metadata=meta,
    )
    comment = odr.build_spotlight_comment(r)
    assert len(comment) <= 1500


def test_enrich_keywords_dedupes_and_caps() -> None:
    r = odr.AnalysisResult(
        title="Alpha",
        summary="The quick brown fox jumps over the lazy dog repeatedly.",
        source_kind="text",
        metadata={
            "keywords": ["alpha", "Alpha"],
            "people": ["Bob"],
            "document_type": "Memo",
        },
    )
    kws = odr.enrich_keywords(r)
    assert len(kws) <= odr.MAX_PDF_KEYWORDS
    assert "Alpha" in kws or "alpha" in [x.casefold() for x in kws]


def test_extract_summary_keywords_stopwords() -> None:
    words = odr.extract_summary_keywords("The and an of test UniqueWordHere")
    assert "UniqueWordHere" in words
    assert "the" not in [w.casefold() for w in words]
