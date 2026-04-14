#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import os
import plistlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Iterable
from urllib.parse import urlparse

from tqdm import tqdm


OLLAMA_BASE_URL = "http://127.0.0.1:11434"
MAX_TEXT_CHARS = 12000
TITLE_MAX_LENGTH = 120
MAX_PDF_KEYWORDS = 24
TEXT_EXTENSIONS = {
    ".txt",
    ".text",
    ".md",
    ".markdown",
    ".csv",
    ".tsv",
    ".json",
    ".xml",
    ".yaml",
    ".yml",
    ".html",
    ".htm",
    ".rtf",
    ".log",
}
OFFICE_EXTENSIONS = {
    ".doc",
    ".docx",
    ".odt",
    ".rtf",
    ".html",
    ".htm",
    ".webarchive",
}
IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".tif",
    ".tiff",
    ".heic",
    ".webp",
}
PDF_EXTENSIONS = {".pdf"}
SKIP_FILENAMES = {".ds_store"}


@dataclass
class AnalysisResult:
    title: str
    summary: str
    source_kind: str
    metadata: dict[str, object]


@dataclass
class ProcessOutcome:
    original_path: Path
    renamed_path: Path | None
    result: AnalysisResult | None
    skipped_reason: str | None = None
    pdf_status: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize files with a local Ollama model and rename each file "
            "to a generated descriptive title."
        )
    )
    parser.add_argument("directory", type=Path, help="Directory to scan recursively")
    parser.add_argument(
        "--model",
        default="llama3.2",
        help="Ollama model for text analysis (default: %(default)s)",
    )
    parser.add_argument(
        "--vision-model",
        default=None,
        help="Ollama model for image/PDF preview analysis (defaults to --model)",
    )
    parser.add_argument(
        "--ollama-url",
        default=OLLAMA_BASE_URL,
        help="Ollama base URL or API URL (default: %(default)s)",
    )
    parser.add_argument(
        "--backend",
        choices=("auto", "http", "cli"),
        default="http",
        help="How to talk to Ollama (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned renames without changing files",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting existing audit log file if present (starts fresh; ignores --resume)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Skip files already recorded in the audit log and append new entries "
            "(use after an interrupted run with the same --audit-log and directory)"
        ),
    )
    parser.add_argument(
        "--audit-log",
        type=Path,
        default=Path("rename_audit.jsonl"),
        help="Where to write JSONL audit output (default: %(default)s)",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden files and directories",
    )
    parser.add_argument(
        "--write-spotlight-comment",
        action="store_true",
        help="Write a macOS Spotlight/Finder comment using the summary and extracted metadata",
    )
    parser.add_argument(
        "--write-pdf-metadata",
        action="store_true",
        help="Conservatively write PDF Info and XMP metadata for unsigned, unencrypted PDFs",
    )
    parser.add_argument(
        "--pdf-backup-suffix",
        default=".metadata-backup.pdf",
        help="Suffix for backup copies created before PDF metadata updates (default: %(default)s)",
    )
    parser.add_argument(
        "--validate-pdf-after-write",
        action="store_true",
        help="Validate PDFs after metadata updates and restore the backup automatically if validation fails",
    )
    parser.add_argument(
        "--delete-pdf-backup-on-success",
        action="store_true",
        help="Delete the PDF backup after a successful metadata write and optional validation",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Limit processing to the first N files",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of files to process in parallel (default: %(default)s)",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable the progress bar (it is off automatically when stderr is not a TTY)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_dir = args.directory.expanduser().resolve()
    audit_log = args.audit_log.expanduser().resolve()
    vision_model = args.vision_model or args.model

    if not target_dir.is_dir():
        print(f"Directory not found: {target_dir}", file=sys.stderr)
        return 1

    if audit_log.exists() and not args.overwrite and not args.dry_run and not args.resume:
        print(
            f"Audit log already exists: {audit_log}. Use --overwrite, --resume, or a different path.",
            file=sys.stderr,
        )
        return 1

    resume_active = args.resume and not args.overwrite
    completed_renamed: set[Path] = set()
    if resume_active:
        completed_renamed = load_completed_renamed_paths(audit_log, target_dir)
        if completed_renamed:
            print(
                f"Resume: loaded {len(completed_renamed)} completed path(s) from {audit_log}",
                file=sys.stderr,
            )

    files = list(iter_files(target_dir, include_hidden=args.include_hidden))
    if completed_renamed:
        before = len(files)
        files = [p for p in files if p.resolve() not in completed_renamed]
        skipped = before - len(files)
        if skipped:
            print(f"Resume: skipping {skipped} file(s) already in the audit log.", file=sys.stderr)
    if args.max_files is not None:
        files = files[: args.max_files]

    if not files:
        print("No files found to process.")
        return 0

    if args.workers < 1:
        print("--workers must be at least 1", file=sys.stderr)
        return 1

    show_progress = sys.stderr.isatty() and not args.no_progress

    audit_handle = None
    write_lock = Lock()
    try:
        if not args.dry_run:
            if args.overwrite or not audit_log.exists():
                audit_handle = audit_log.open("w", encoding="utf-8")
            elif args.resume:
                audit_handle = audit_log.open("a", encoding="utf-8")
            else:
                audit_handle = audit_log.open("w", encoding="utf-8")

        if args.workers == 1:
            file_iter: Iterable[Path] = files
            if show_progress:
                file_iter = tqdm(
                    files,
                    total=len(files),
                    unit="file",
                    desc="Renaming",
                    file=sys.stderr,
                )
            for file_path in file_iter:
                outcome = process_file(
                    file_path=file_path,
                    text_model=args.model,
                    vision_model=vision_model,
                    ollama_url=args.ollama_url,
                    backend=args.backend,
                    dry_run=args.dry_run,
                    write_spotlight_comment_flag=args.write_spotlight_comment,
                    write_pdf_metadata_flag=args.write_pdf_metadata,
                    pdf_backup_suffix=args.pdf_backup_suffix,
                    validate_pdf_after_write=args.validate_pdf_after_write,
                    delete_pdf_backup_on_success=args.delete_pdf_backup_on_success,
                    audit_handle=audit_handle,
                    write_lock=write_lock,
                )
                report_outcome(outcome, dry_run=args.dry_run)
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [
                    executor.submit(
                        process_file,
                        file_path=file_path,
                        text_model=args.model,
                        vision_model=vision_model,
                        ollama_url=args.ollama_url,
                        backend=args.backend,
                        dry_run=args.dry_run,
                        write_spotlight_comment_flag=args.write_spotlight_comment,
                        write_pdf_metadata_flag=args.write_pdf_metadata,
                        pdf_backup_suffix=args.pdf_backup_suffix,
                        validate_pdf_after_write=args.validate_pdf_after_write,
                        delete_pdf_backup_on_success=args.delete_pdf_backup_on_success,
                        audit_handle=audit_handle,
                        write_lock=write_lock,
                    )
                    for file_path in files
                ]
                if show_progress:
                    with tqdm(
                        total=len(futures),
                        unit="file",
                        desc="Renaming",
                        file=sys.stderr,
                    ) as pbar:
                        for future in as_completed(futures):
                            outcome = future.result()
                            report_outcome(outcome, dry_run=args.dry_run)
                            pbar.update(1)
                else:
                    for future in as_completed(futures):
                        outcome = future.result()
                        report_outcome(outcome, dry_run=args.dry_run)
    finally:
        if audit_handle is not None:
            audit_handle.close()

    return 0


def load_completed_renamed_paths(audit_path: Path, root: Path) -> set[Path]:
    """Resolved paths of outputs already recorded in the audit (successful renames)."""
    root_resolved = root.resolve()
    completed: set[Path] = set()
    if not audit_path.is_file():
        return completed
    try:
        with audit_path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                renamed = row.get("renamed_path")
                if not renamed or not isinstance(renamed, str):
                    continue
                try:
                    p = Path(renamed).expanduser().resolve()
                except OSError:
                    continue
                try:
                    p.relative_to(root_resolved)
                except ValueError:
                    continue
                completed.add(p)
    except OSError:
        return completed
    return completed


def iter_files(root: Path, include_hidden: bool) -> Iterable[Path]:
    for current_root, dirnames, filenames in os.walk(root):
        current_path = Path(current_root)
        if not include_hidden:
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for filename in filenames:
            if not include_hidden and filename.startswith("."):
                continue
            if filename.lower() in SKIP_FILENAMES:
                continue
            path = current_path / filename
            if path.is_file():
                yield path


def process_file(
    file_path: Path,
    text_model: str,
    vision_model: str,
    ollama_url: str,
    backend: str,
    dry_run: bool,
    write_spotlight_comment_flag: bool,
    write_pdf_metadata_flag: bool,
    pdf_backup_suffix: str,
    validate_pdf_after_write: bool,
    delete_pdf_backup_on_success: bool,
    audit_handle,
    write_lock: Lock,
) -> ProcessOutcome:
    try:
        result = analyze_file(
            file_path=file_path,
            text_model=text_model,
            vision_model=vision_model,
            ollama_url=ollama_url,
            backend=backend,
        )
    except Exception as exc:  # noqa: BLE001
        return ProcessOutcome(
            original_path=file_path,
            renamed_path=None,
            result=None,
            skipped_reason=str(exc),
        )

    safe_title = sanitize_title(result.title)
    if not safe_title:
        return ProcessOutcome(
            original_path=file_path,
            renamed_path=None,
            result=result,
            skipped_reason="model returned an empty title",
        )

    with write_lock:
        destination = unique_destination(file_path, safe_title)
        pdf_status = None

        if not dry_run:
            file_path.rename(destination)
            if write_spotlight_comment_flag:
                write_spotlight_comment(destination, build_spotlight_comment(result))
            if write_pdf_metadata_flag and destination.suffix.lower() in PDF_EXTENSIONS:
                try:
                    pdf_status = write_pdf_metadata_conservatively(
                        file_path=destination,
                        result=result,
                        backup_suffix=pdf_backup_suffix,
                        validate_after_write=validate_pdf_after_write,
                        delete_backup_on_success=delete_pdf_backup_on_success,
                    )
                except RuntimeError as exc:
                    pdf_status = f"skipped ({exc})"

            if audit_handle is not None:
                audit_handle.write(
                    json.dumps(
                        {
                            "original_path": str(file_path),
                            "renamed_path": str(destination),
                            "summary": result.summary,
                            "title": result.title,
                            "source_kind": result.source_kind,
                            "metadata": result.metadata,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                audit_handle.flush()

    return ProcessOutcome(
        original_path=file_path,
        renamed_path=destination,
        result=result,
        pdf_status=pdf_status,
    )


def report_outcome(outcome: ProcessOutcome, dry_run: bool) -> None:
    if outcome.skipped_reason:
        print(f"SKIP {outcome.original_path.name}: {outcome.skipped_reason}", file=sys.stderr)
        return

    assert outcome.renamed_path is not None
    assert outcome.result is not None

    action = "WOULD RENAME" if dry_run else "RENAMED"
    print(f"{action}: {outcome.original_path.name} -> {outcome.renamed_path.name}")
    print(f"  Summary: {outcome.result.summary}")
    metadata_preview = format_metadata_preview(outcome.result.metadata)
    if metadata_preview:
        print(f"  Metadata: {metadata_preview}")
    if outcome.pdf_status:
        print(f"  PDF metadata: {outcome.pdf_status}")


def analyze_file(
    file_path: Path,
    text_model: str,
    vision_model: str,
    ollama_url: str,
    backend: str,
) -> AnalysisResult:
    suffix = file_path.suffix.lower()
    text = extract_text(file_path)
    if text:
        return analyze_text_with_ollama(
            text=text,
            text_model=text_model,
            ollama_url=ollama_url,
            filename=file_path.name,
            source_kind="text",
            backend=backend,
        )

    if suffix in IMAGE_EXTENSIONS:
        ocr_text = extract_text_with_ocr(file_path)
        if meaningful_text(ocr_text):
            return analyze_text_with_ollama(
                text=truncate_text(ocr_text),
                text_model=text_model,
                ollama_url=ollama_url,
                filename=file_path.name,
                source_kind="ocr-image",
                backend=backend,
            )

    if suffix in IMAGE_EXTENSIONS:
        if backend == "cli":
            raise RuntimeError("image analysis requires the HTTP Ollama API; CLI backend is text-only")
        return analyze_image_with_ollama(
            image_path=file_path,
            vision_model=vision_model,
            ollama_url=ollama_url,
            filename=file_path.name,
            source_kind="image",
            backend=backend,
        )

    if suffix in PDF_EXTENSIONS:
        preview_path = render_pdf_preview(file_path)
        try:
            ocr_text = extract_text_with_ocr(preview_path)
            if meaningful_text(ocr_text):
                return analyze_text_with_ollama(
                    text=truncate_text(ocr_text),
                    text_model=text_model,
                    ollama_url=ollama_url,
                    filename=file_path.name,
                    source_kind="ocr-pdf-preview",
                    backend=backend,
                )

            if backend == "cli":
                raise RuntimeError(
                    "PDF did not yield usable text from OCR, and CLI backend cannot do visual analysis"
                )
            return analyze_image_with_ollama(
                image_path=preview_path,
                vision_model=vision_model,
                ollama_url=ollama_url,
                filename=file_path.name,
                source_kind="pdf-preview",
                backend=backend,
            )
        finally:
            if preview_path.exists():
                preview_path.unlink()

    raise RuntimeError("no supported extraction path found")


def extract_text(file_path: Path) -> str:
    candidates = [
        extract_plain_text,
        extract_with_textutil,
        extract_with_mdls,
    ]
    for extractor in candidates:
        text = extractor(file_path)
        if meaningful_text(text):
            return truncate_text(text)
    return ""


def extract_text_with_ocr(image_path: Path) -> str:
    swift_path = Path(__file__).with_name("macos_ocr.swift")
    if not swift_path.exists():
        return ""
    if not shutil.which("swift"):
        return ""
    try:
        completed = subprocess.run(
            ["swift", str(swift_path), str(image_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()


def extract_plain_text(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if suffix not in TEXT_EXTENSIONS and not (mime_type and mime_type.startswith("text/")):
        return ""
    try:
        return file_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


def extract_with_textutil(file_path: Path) -> str:
    if not shutil.which("textutil"):
        return ""
    suffix = file_path.suffix.lower()
    if suffix not in OFFICE_EXTENSIONS:
        return ""
    try:
        completed = subprocess.run(
            ["textutil", "-convert", "txt", "-stdout", str(file_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    if completed.returncode != 0:
        return ""
    return completed.stdout


def extract_with_mdls(file_path: Path) -> str:
    if not shutil.which("mdls"):
        return ""
    try:
        completed = subprocess.run(
            ["mdls", "-raw", "-name", "kMDItemTextContent", str(file_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    if completed.returncode != 0:
        return ""
    output = completed.stdout.strip()
    if output in {"(null)", ""}:
        return ""
    return output


def meaningful_text(text: str) -> bool:
    collapsed = re.sub(r"\s+", " ", text or "").strip()
    return len(collapsed) >= 80


def truncate_text(text: str) -> str:
    collapsed = re.sub(r"\s+", " ", text).strip()
    return collapsed[:MAX_TEXT_CHARS]


def render_pdf_preview(file_path: Path) -> Path:
    if not shutil.which("qlmanage"):
        raise RuntimeError("qlmanage is not available for PDF preview rendering")

    with tempfile.TemporaryDirectory() as temp_dir:
        completed = subprocess.run(
            [
                "qlmanage",
                "-t",
                "-s",
                "1600",
                "-o",
                temp_dir,
                str(file_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"unable to render PDF preview: {completed.stderr.strip() or completed.stdout.strip()}"
            )

        generated = next(Path(temp_dir).glob("*.png"), None)
        if generated is None:
            raise RuntimeError("PDF preview rendering did not create a PNG")

        preview_copy = Path(tempfile.mkstemp(suffix=".png")[1])
        shutil.copy2(generated, preview_copy)
        return preview_copy


def analyze_text_with_ollama(
    text: str,
    text_model: str,
    ollama_url: str,
    filename: str,
    source_kind: str,
    backend: str,
) -> AnalysisResult:
    prompt = (
        "You are renaming archived documents.\n"
        "Read the file content and produce a concise descriptive title, summary, and searchable metadata.\n"
        "Rules:\n"
        "- Title must be 3 to 10 words.\n"
        "- Title must be specific, human-readable, and suitable for a filename.\n"
        "- Do not include the original numeric filename.\n"
        "- Do not use slashes, colons, quotes, or trailing punctuation.\n"
        "- Summary must be 1 or 2 sentences.\n"
        "- Metadata must be factual and conservative. Prefer empty arrays or empty strings over guessing.\n"
        "- Keywords should be short search terms, ideally 3 to 8 items.\n"
        "- If the document is ambiguous, choose the best factual title you can.\n"
        "Respond with JSON only using keys title, summary, and metadata.\n"
        "Metadata must be an object with keys: document_type, people, organizations, locations, dates, keywords, identifiers, language.\n\n"
        f"Filename: {filename}\n"
        f"Content:\n{text}"
    )
    payload = {
        "model": text_model,
        "format": "json",
        "stream": False,
        "messages": [{"role": "user", "content": prompt}],
        "prompt": prompt,
    }
    response = ollama_chat(payload, ollama_url, backend)
    return AnalysisResult(
        title=response["title"].strip(),
        summary=clean_summary(response["summary"]),
        source_kind=source_kind,
        metadata=normalize_metadata(response.get("metadata")),
    )


def analyze_image_with_ollama(
    image_path: Path,
    vision_model: str,
    ollama_url: str,
    filename: str,
    source_kind: str,
    backend: str,
) -> AnalysisResult:
    image_b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
    prompt = (
        "You are renaming archived scanned documents.\n"
        "Look at the image and infer the most accurate concise title, summary, and searchable metadata you can.\n"
        "Rules:\n"
        "- Title must be 3 to 10 words.\n"
        "- Title must be specific and suitable for a filename.\n"
        "- Do not use slashes, colons, quotes, or trailing punctuation.\n"
        "- Summary must be 1 or 2 sentences.\n"
        "- Metadata must be factual and conservative. Prefer empty arrays or empty strings over guessing.\n"
        "- Keywords should be short search terms, ideally 3 to 8 items.\n"
        "- If some text is unreadable, use the visible clues and stay conservative.\n"
        "Respond with JSON only using keys title, summary, and metadata.\n"
        "Metadata must be an object with keys: document_type, people, organizations, locations, dates, keywords, identifiers, language.\n\n"
        f"Original filename: {filename}"
    )
    payload = {
        "model": vision_model,
        "format": "json",
        "stream": False,
        "messages": [{"role": "user", "content": prompt, "images": [image_b64]}],
        "prompt": prompt,
        "images": [image_b64],
    }
    response = ollama_chat(payload, ollama_url, backend)
    return AnalysisResult(
        title=response["title"].strip(),
        summary=clean_summary(response["summary"]),
        source_kind=source_kind,
        metadata=normalize_metadata(response.get("metadata")),
    )


def ollama_chat(payload: dict, ollama_url: str, backend: str) -> dict:
    if backend in {"auto", "http"}:
        try:
            return ollama_chat_http(payload, ollama_url)
        except RuntimeError:
            if backend == "http":
                raise
    if backend in {"auto", "cli"}:
        return ollama_chat_cli(payload)
    raise RuntimeError(f"unsupported backend: {backend}")


def ollama_chat_http(payload: dict, ollama_url: str) -> dict:
    ensure_model_available(payload["model"], ollama_url)
    chat_url = ollama_api_url(ollama_url, "/api/chat")
    try:
        raw = post_json(chat_url, build_chat_payload(payload))
        return parse_ollama_response(raw, "chat")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"Ollama chat request failed at {chat_url} with HTTP {exc.code}: {detail[:300]}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            "unable to reach Ollama at "
            f"{chat_url}. Make sure the local Ollama service is available. Details: {exc}"
        ) from exc


def ollama_chat_cli(payload: dict) -> dict:
    if not shutil.which("ollama"):
        raise RuntimeError("`ollama` CLI is not installed or not on PATH")

    prompt = payload["prompt"] + (
        "\n\nReturn valid JSON only. No markdown fences, no extra commentary."
    )
    command = ["ollama", "run", payload["model"], prompt]
    if payload.get("images"):
        raise RuntimeError(
            "CLI fallback does not support image inputs. Use the HTTP backend with a vision-capable Ollama server."
        )

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise RuntimeError(f"unable to execute `ollama run`: {exc}") from exc

    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"`ollama run` failed: {detail[:400]}")

    raw = completed.stdout.strip()
    try:
        inner = parse_lenient_json_response(raw)
    except RuntimeError as exc:
        raise RuntimeError(f"unexpected CLI response: {raw[:400]}") from exc

    if not inner.get("title") or not inner.get("summary"):
        raise RuntimeError(f"incomplete Ollama CLI response: {raw[:400]}")
    return inner


def extract_json_object(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise json.JSONDecodeError("No JSON object found", text, 0)
    return text[start : end + 1]


def parse_lenient_json_response(raw: str) -> dict:
    try:
        return json.loads(extract_json_object(raw))
    except json.JSONDecodeError:
        pass

    json_text = extract_json_object(raw)
    title_match = re.search(r'"title"\s*:\s*"(?P<value>.*?)"\s*,', json_text, re.DOTALL)
    summary_match = re.search(r'"summary"\s*:\s*"(?P<value>.*?)"\s*(,|\})', json_text, re.DOTALL)
    if not title_match or not summary_match:
        raise RuntimeError("could not parse title/summary from CLI response")

    parsed = {
        "title": clean_model_string(title_match.group("value")),
        "summary": clean_model_string(summary_match.group("value")),
    }
    try:
        parsed["metadata"] = extract_metadata_object(json.loads(json_text))
    except (RuntimeError, json.JSONDecodeError):
        parsed["metadata"] = {}
    return parsed


def clean_model_string(value: str) -> str:
    value = value.replace('\\"', '"')
    value = value.replace("\\n", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def ollama_api_url(ollama_url: str, path: str) -> str:
    parsed = urlparse(ollama_url)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(f"invalid Ollama URL: {ollama_url}")
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}{path}"


def ensure_model_available(model: str, ollama_url: str) -> None:
    available_models = fetch_available_models(ollama_url)
    if not available_models:
        return
    if model in available_models:
        return

    normalized = {normalize_model_name(name): name for name in available_models}
    candidate = normalized.get(normalize_model_name(model))
    if candidate:
        raise RuntimeError(
            f"model `{model}` is not installed, but `{candidate}` is available. Use `--model {candidate}`"
        )

    raise RuntimeError(
        f"model `{model}` is not installed. Available models: {', '.join(sorted(available_models))}"
    )


def fetch_available_models(ollama_url: str) -> list[str]:
    tags_url = ollama_api_url(ollama_url, "/api/tags")
    request = urllib.request.Request(tags_url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.URLError:
        return []

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []

    models = payload.get("models", [])
    names: list[str] = []
    for model in models:
        name = model.get("name")
        if isinstance(name, str) and name:
            names.append(name)
    return names


def normalize_model_name(name: str) -> str:
    lowered = name.strip().lower()
    if ":" not in lowered:
        lowered = f"{lowered}:latest"
    return lowered


def build_chat_payload(payload: dict) -> dict:
    chat_payload = {
        "model": payload["model"],
        "stream": False,
        "format": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "summary": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {
                        "document_type": {"type": "string"},
                        "people": {"type": "array", "items": {"type": "string"}},
                        "organizations": {"type": "array", "items": {"type": "string"}},
                        "locations": {"type": "array", "items": {"type": "string"}},
                        "dates": {"type": "array", "items": {"type": "string"}},
                        "keywords": {"type": "array", "items": {"type": "string"}},
                        "identifiers": {"type": "array", "items": {"type": "string"}},
                        "language": {"type": "string"},
                    },
                    "required": [
                        "document_type",
                        "people",
                        "organizations",
                        "locations",
                        "dates",
                        "keywords",
                        "identifiers",
                        "language",
                    ],
                },
            },
            "required": ["title", "summary", "metadata"],
        },
        "options": {"temperature": 0.2},
        "messages": payload["messages"],
    }
    return chat_payload


def post_json(url: str, payload: dict) -> str:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        return response.read().decode("utf-8")


def parse_ollama_response(raw: str, mode: str) -> dict:
    try:
        outer = json.loads(raw)
        if mode == "chat":
            content = outer["message"]["content"]
        elif mode == "generate":
            content = outer["response"]
        else:
            raise KeyError(mode)
        inner = json.loads(content)
    except (KeyError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"unexpected Ollama response: {raw[:400]}") from exc

    if not inner.get("title") or not inner.get("summary"):
        raise RuntimeError(f"incomplete Ollama response: {content}")
    inner["metadata"] = normalize_metadata(inner.get("metadata"))
    return inner


def sanitize_title(title: str) -> str:
    title = title.strip()
    title = re.sub(r"[\\/:*?\"<>|]+", " ", title)
    title = re.sub(r"\s+", " ", title)
    title = title.strip(" ._-")
    title = title[:TITLE_MAX_LENGTH].rstrip(" ._-")
    return title


def clean_summary(summary: str) -> str:
    summary = re.sub(r"\s+", " ", summary).strip()
    return summary[:400]


def enrich_keywords(result: AnalysisResult) -> list[str]:
    metadata = result.metadata
    candidates: list[str] = []

    for field_name in ("keywords", "people", "organizations", "locations", "dates", "identifiers"):
        value = metadata.get(field_name)
        if isinstance(value, list):
            candidates.extend(item for item in value if isinstance(item, str))

    for field_name in ("document_type", "language"):
        value = metadata.get(field_name)
        if isinstance(value, str) and value:
            candidates.append(value)

    candidates.append(result.title)
    candidates.extend(extract_summary_keywords(result.summary))

    cleaned: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        entry = re.sub(r"\s+", " ", item).strip(" .,;:-")
        if not entry:
            continue
        dedupe_key = entry.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        cleaned.append(entry[:120])
        if len(cleaned) >= MAX_PDF_KEYWORDS:
            break
    return cleaned


def extract_summary_keywords(summary: str) -> list[str]:
    stopwords = {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "in",
        "is",
        "it",
        "of",
        "on",
        "or",
        "that",
        "the",
        "this",
        "to",
        "with",
    }
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9&'._-]{2,}", summary)
    keywords: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        normalized = token.casefold()
        if normalized in stopwords:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        keywords.append(token[:80])
        if len(keywords) >= 12:
            break
    return keywords


def extract_metadata_object(payload: dict) -> dict:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    return {}


def normalize_metadata(metadata: object) -> dict[str, object]:
    if not isinstance(metadata, dict):
        metadata = {}

    normalized = {
        "document_type": normalize_scalar(metadata.get("document_type"), max_length=80),
        "people": normalize_string_list(metadata.get("people"), max_items=12),
        "organizations": normalize_string_list(metadata.get("organizations"), max_items=12),
        "locations": normalize_string_list(metadata.get("locations"), max_items=12),
        "dates": normalize_string_list(metadata.get("dates"), max_items=12),
        "keywords": normalize_string_list(metadata.get("keywords"), max_items=12),
        "identifiers": normalize_string_list(metadata.get("identifiers"), max_items=12),
        "language": normalize_scalar(metadata.get("language"), max_length=40),
    }
    return normalized


def normalize_scalar(value: object, max_length: int) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned[:max_length]


def normalize_string_list(value: object, max_items: int) -> list[str]:
    if isinstance(value, str):
        value = re.split(r"[;,]\s*|\n+", value)
    if not isinstance(value, list):
        return []

    cleaned: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        entry = re.sub(r"\s+", " ", item).strip(" .,;:-")
        if not entry:
            continue
        dedupe_key = entry.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        cleaned.append(entry[:120])
        if len(cleaned) >= max_items:
            break
    return cleaned


def format_metadata_preview(metadata: dict[str, object]) -> str:
    parts: list[str] = []
    document_type = metadata.get("document_type")
    if isinstance(document_type, str) and document_type:
        parts.append(f"type={document_type}")
    keywords = metadata.get("keywords")
    if isinstance(keywords, list) and keywords:
        parts.append("keywords=" + ", ".join(keywords[:5]))
    people = metadata.get("people")
    if isinstance(people, list) and people:
        parts.append("people=" + ", ".join(people[:3]))
    return "; ".join(parts)


def build_spotlight_comment(result: AnalysisResult) -> str:
    lines = [result.summary]
    metadata = result.metadata

    keyword_line = join_metadata_values("Keywords", metadata.get("keywords"))
    if keyword_line:
        lines.append(keyword_line)
    people_line = join_metadata_values("People", metadata.get("people"))
    if people_line:
        lines.append(people_line)
    organization_line = join_metadata_values("Organizations", metadata.get("organizations"))
    if organization_line:
        lines.append(organization_line)
    location_line = join_metadata_values("Locations", metadata.get("locations"))
    if location_line:
        lines.append(location_line)
    date_line = join_metadata_values("Dates", metadata.get("dates"))
    if date_line:
        lines.append(date_line)
    identifier_line = join_metadata_values("Identifiers", metadata.get("identifiers"))
    if identifier_line:
        lines.append(identifier_line)

    document_type = metadata.get("document_type")
    if isinstance(document_type, str) and document_type:
        lines.append(f"Type: {document_type}")
    language = metadata.get("language")
    if isinstance(language, str) and language:
        lines.append(f"Language: {language}")

    return "\n".join(lines)[:1500]


def join_metadata_values(label: str, value: object) -> str:
    if not isinstance(value, list) or not value:
        return ""
    return f"{label}: {', '.join(value[:8])}"


def write_spotlight_comment(file_path: Path, comment: str) -> None:
    if not comment:
        return
    if not shutil.which("xattr"):
        raise RuntimeError("`xattr` is not available for writing Spotlight comments")

    plist_bytes = plistlib.dumps(comment, fmt=plistlib.FMT_BINARY)
    hex_payload = plist_bytes.hex()
    completed = subprocess.run(
        ["xattr", "-wx", "com.apple.metadata:kMDItemFinderComment", hex_payload, str(file_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"unable to write Spotlight comment: {detail[:300]}")


def write_pdf_metadata_conservatively(
    file_path: Path,
    result: AnalysisResult,
    backup_suffix: str,
    validate_after_write: bool,
    delete_backup_on_success: bool,
) -> str:
    if not shutil.which("exiftool"):
        raise RuntimeError("`exiftool` is required for PDF metadata writing")

    pdf_state = inspect_pdf_safety(file_path)
    if pdf_state["is_encrypted"]:
        raise RuntimeError("skipping PDF metadata update because the file appears encrypted")
    if pdf_state["has_digital_signature"]:
        raise RuntimeError("skipping PDF metadata update because the file appears digitally signed")

    backup_path = build_pdf_backup_path(file_path, backup_suffix)
    if backup_path.exists():
        raise RuntimeError(
            f"backup file already exists at {backup_path}; move it or choose a different --pdf-backup-suffix"
        )
    shutil.copy2(file_path, backup_path)

    subject = result.summary[:250]
    keywords = enrich_keywords(result)
    keyword_text = ", ".join(keywords)

    command = [
        "exiftool",
        "-overwrite_original",
        "-PDF:Title=" + result.title,
        "-PDF:Subject=" + subject,
        "-PDF:Keywords=" + keyword_text,
        "-XMP-dc:Title=" + result.title,
        "-XMP-dc:Description=" + result.summary,
        "-XMP-pdf:Keywords=" + keyword_text,
        "-XMP-dc:Subject=" + keyword_text,
        "-XMP-xmp:Label=" + normalize_scalar(result.metadata.get("document_type"), max_length=80),
        "-XMP-xmp:Nickname=" + result.title,
        "-XMP-photoshop:Headline=" + result.title,
        str(file_path),
    ]

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        shutil.copy2(backup_path, file_path)
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"unable to write PDF metadata: {detail[:400]}")

    status_parts = [f"written with backup at {backup_path.name}"]

    if validate_after_write:
        validation_error = validate_pdf_after_write(file_path)
        if validation_error:
            shutil.copy2(backup_path, file_path)
            raise RuntimeError(
                "PDF validation failed after metadata update; the original file was restored "
                f"from backup. Details: {validation_error}"
            )
        status_parts.append("validated")

    if delete_backup_on_success:
        try:
            backup_path.unlink()
        except OSError as exc:
            raise RuntimeError(
                f"metadata was written successfully, but the backup could not be deleted: {exc}"
            ) from exc
        status_parts.append("backup deleted")

    return ", ".join(status_parts)


def build_pdf_backup_path(file_path: Path, backup_suffix: str) -> Path:
    suffix = backup_suffix or ".metadata-backup.pdf"
    if not suffix.lower().endswith(".pdf"):
        suffix = f"{suffix}.pdf"
    return file_path.with_name(f"{file_path.stem}{suffix}")


def inspect_pdf_safety(file_path: Path) -> dict[str, bool]:
    try:
        sample = file_path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"unable to inspect PDF before metadata update: {exc}") from exc

    has_signature = bool(
        re.search(rb"/ByteRange\s*\[", sample)
        or re.search(rb"/Type\s*/Sig\b", sample)
        or re.search(rb"/FT\s*/Sig\b", sample)
    )
    is_encrypted = bool(re.search(rb"/Encrypt\b", sample))
    return {
        "has_digital_signature": has_signature,
        "is_encrypted": is_encrypted,
    }


def validate_pdf_after_write(file_path: Path) -> str:
    validators = [
        validate_pdf_with_exiftool,
        validate_pdf_with_mdls,
        validate_pdf_with_qlmanage,
    ]
    errors: list[str] = []
    for validator in validators:
        error = validator(file_path)
        if error:
            errors.append(error)
    return "; ".join(errors)


def validate_pdf_with_exiftool(file_path: Path) -> str:
    if not shutil.which("exiftool"):
        return ""
    completed = subprocess.run(
        ["exiftool", "-s", "-PDF:Title", str(file_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        return f"exiftool could not read the PDF metadata ({detail[:250]})"
    return ""


def validate_pdf_with_mdls(file_path: Path) -> str:
    if not shutil.which("mdls"):
        return ""
    last_detail = ""
    for _ in range(3):
        completed = subprocess.run(
            ["mdls", "-name", "kMDItemContentType", str(file_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            output = completed.stdout.strip()
            if "com.adobe.pdf" in output:
                return ""
            last_detail = f"mdls did not report a PDF content type ({output[:250]})"
        else:
            last_detail = (completed.stderr or completed.stdout).strip()
            if "could not find" not in last_detail.casefold():
                break
        time.sleep(0.2)
    if file_path.exists() and "could not find" in last_detail.casefold():
        return ""
    if last_detail:
        return f"mdls could not inspect the PDF ({last_detail[:250]})"
    return ""


def validate_pdf_with_qlmanage(file_path: Path) -> str:
    if not shutil.which("qlmanage"):
        return ""
    with tempfile.TemporaryDirectory() as temp_dir:
        completed = subprocess.run(
            [
                "qlmanage",
                "-t",
                "-s",
                "256",
                "-o",
                temp_dir,
                str(file_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            if "sandbox initialization failed" in detail.casefold():
                return ""
            return f"Quick Look could not render the PDF ({detail[:250]})"
        generated = next(Path(temp_dir).glob("*.png"), None)
        if generated is None:
            return "Quick Look did not generate a preview image"
    return ""


def unique_destination(source: Path, title: str) -> Path:
    extension = source.suffix
    candidate = source.with_name(f"{title}{extension}")
    counter = 2
    while candidate.exists() and candidate != source:
        candidate = source.with_name(f"{title} {counter}{extension}")
        counter += 1
    return candidate


if __name__ == "__main__":
    raise SystemExit(main())
