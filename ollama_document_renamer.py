#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import fnmatch
import json
import signal
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
from concurrent.futures import FIRST_COMPLETED, CancelledError, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock
from typing import Iterable, Sequence
from urllib.parse import urlparse

from tqdm import tqdm


OLLAMA_BASE_URL = "http://127.0.0.1:11434"
MAX_TEXT_CHARS = 12000
TITLE_MAX_LENGTH = 120
MAX_PDF_KEYWORDS = 24

# Shared instructions for Ollama document analysis (text and vision paths).
DOCUMENT_ANALYSIS_PROMPT_RULES = """
Optimize for search and indexing first. The title becomes the filename (the file extension is preserved separately): it must be easy for a human to scan in a folder list and easy for desktop search, Spotlight, and PDF/metadata indexes to match real queries.

The summary and metadata are equally important for search: many tools index title, description or subject, and keywords separately. Align wording so the same high-value terms appear where appropriate across title, summary, keywords, dates, identifiers, people, organizations, and document_type.

Title (filename stem) rules:
- Write the title as a compact line of high-signal tokens, not a sentence. Prefer this order when the facts exist: primary date or period anchor, then document type, then main parties or entities, then distinguishing identifiers (invoice number, case number, account suffix, policy number, form name and tax year, etc.).
- Use plain language and real names as shown on the document. Do not use generic fillers such as: document, scan, copy, misc, important, new, final, updated, unless needed to disambiguate two otherwise identical files.
- Aim for roughly 4 to 12 short segments (words or hyphenated tokens); stay specific. If the content supports it, include a year or ISO date (YYYY-MM-DD) in the title when that helps recall.
- Imagine 3 to 5 realistic search queries someone would type to find this file later; the title should contain the main distinctive terms from those queries (names, organizations, form types, years, key numbers).
- Title must be suitable for a filename. Do not use slashes, colons, quotes, or trailing punctuation.

Metadata rules for search:
- document_type: use a short canonical token (for example pay-stub, bank-statement, w-2, 1099-int, lease-agreement) and rely on keywords for brand-specific or long variants.
- dates: include ISO dates (YYYY-MM-DD) when known. Also include salient non-ISO phrases from the document when useful (for example pay period ending, statement month, tax year). Prefer multiple entries when the document supplies multiple relevant dates.
- identifiers: capture every stable reference that someone might search (invoice number, case number, policy number, confirmation code, last-4 of account, employee ID, ticket number). Use empty only when none appear.
- people and organizations: include all prominent names as on the document; add a second spelling or obvious alternate only when it improves search (for example legal name versus common trade name) and is supported by the text.
- keywords: 5 to 12 short terms. Include abbreviations, form codes, product names, alternate spellings, and synonymous labels a user might type (for example W-2, W2, Form W-2 when applicable). Terms in the title should generally also appear in keywords or structured fields so different index fields reinforce each other.
- locations: include when they would plausibly appear in a search (city or state, property address fragment) and appear on the document.
- language: set accurately; if mixed-language, note that in keywords.
- Metadata must be factual and conservative. Prefer empty arrays or empty strings over guessing.

Summary rules:
- One or two sentences that stay factual and repeat the most search-important entities, dates, and identifiers already captured elsewhere (many systems index Subject or Description fields).
- Avoid introducing new claims not supported by the document.

Respond with JSON only using keys title, summary, and metadata.
Metadata must be an object with keys: document_type, people, organizations, locations, dates, keywords, identifiers, language.
""".strip()
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
    pdf_repair_status: str | None = None


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
        "--exclude-glob",
        action="append",
        default=None,
        metavar="PATTERN",
        help=(
            "Skip directories (do not descend) and files matching this glob; "
            "repeatable. Shell-style globs (fnmatch). If PATTERN contains no '/', "
            "it matches only the final path component (e.g. node_modules, *.tmp). "
            "If it contains '/', it matches the whole path relative to the scan root."
        ),
    )
    parser.add_argument(
        "--exclude-regex",
        action="append",
        default=None,
        metavar="PATTERN",
        help=(
            "Skip directories and files whose path relative to the scan root matches "
            "this Python regex (repeatable); paths use '/' separators."
        ),
    )
    parser.add_argument(
        "--exclude-git-repos",
        action="store_true",
        help=(
            "Do not descend into subdirectories that are Git repository roots "
            "(contain a .git file or directory). The scan root itself is still scanned."
        ),
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
        "--pdf-preview-page",
        type=int,
        default=1,
        metavar="N",
        help=(
            "For PDF OCR and vision preview, render this 1-based page (default: %(default)s). "
            "Use when the first pages are blank covers or placeholders."
        ),
    )
    parser.add_argument(
        "--repair-pdf-if-needed",
        action="store_true",
        help=(
            "Before analyzing each PDF, detect files that exiftool cannot parse (for example invalid xref) "
            "or that fail qpdf --check, and rewrite them in place with qpdf. Creates a backup beside the file. "
            "Requires qpdf in PATH (for example: brew install qpdf)."
        ),
    )
    parser.add_argument(
        "--pdf-repair-backup-suffix",
        default=".qpdf-repair-backup.pdf",
        help="Suffix for backups created before qpdf repair (default: %(default)s)",
    )
    parser.add_argument(
        "--repair-pdf-macos-pdfkit",
        action="store_true",
        help=(
            "When a PDF is unreadable by exiftool, resave it with macOS PDFKit (same engine family as "
            "Preview). Use alone, or with --repair-pdf-if-needed to run qpdf first and PDFKit only if "
            "still unreadable. Requires swift and macos_pdf_resave.swift beside the script."
        ),
    )
    parser.add_argument(
        "--pdf-pdfkit-repair-backup-suffix",
        default=".pdfkit-repair-backup.pdf",
        help="Suffix for backups created before a PDFKit resave (default: %(default)s)",
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


def _install_graceful_interrupt_handlers(shutdown: Event) -> tuple[object, object | None]:
    def handler(signum: int, frame) -> None:  # noqa: ARG001
        if signum == signal.SIGINT and shutdown.is_set():
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.raise_signal(signal.SIGINT)
        if not shutdown.is_set():
            print(
                "\nInterrupt: stopping; in-flight work will finish, queued work is cancelled.",
                file=sys.stderr,
            )
        shutdown.set()

    old_int = signal.signal(signal.SIGINT, handler)
    old_term = None
    if hasattr(signal, "SIGTERM"):
        old_term = signal.signal(signal.SIGTERM, handler)
    return old_int, old_term


def _restore_graceful_interrupt_handlers(old_sigint: object, old_sigterm: object | None) -> None:
    signal.signal(signal.SIGINT, old_sigint)
    if old_sigterm is not None:
        signal.signal(signal.SIGTERM, old_sigterm)


def _drain_executor_futures(
    futures: list,
    shutdown: Event,
    *,
    dry_run: bool,
    pbar: object | None,
) -> bool:
    """Process futures as they complete; return True if stopped early due to interrupt."""
    pending = set(futures)
    interrupted = False
    while pending:
        completed, pending = wait(pending, timeout=0.25, return_when=FIRST_COMPLETED)
        # Cancelled futures may be `done()` without appearing in `completed` on some
        # Python versions, which would otherwise spin until timeout forever.
        for f in list(pending):
            if f.done():
                pending.remove(f)
                completed.add(f)
        for future in completed:
            try:
                outcome = future.result()
            except CancelledError:
                if pbar is not None:
                    pbar.update(1)
                continue
            report_outcome(outcome, dry_run=dry_run)
            if pbar is not None:
                pbar.update(1)
        if shutdown.is_set():
            interrupted = True
            break
    return interrupted


def main() -> int:
    args = parse_args()
    target_dir = args.directory.expanduser().resolve()
    audit_log = args.audit_log.expanduser().resolve()
    vision_model = args.vision_model or args.model
    exclude_globs = tuple(args.exclude_glob or ())
    exclude_regexes: list[re.Pattern[str]] = []
    for pat in args.exclude_regex or ():
        try:
            exclude_regexes.append(re.compile(pat))
        except re.error as exc:
            print(f"Invalid --exclude-regex {pat!r}: {exc}", file=sys.stderr)
            return 1

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

    files = list(
        iter_files(
            target_dir,
            include_hidden=args.include_hidden,
            exclude_globs=exclude_globs,
            exclude_regexes=exclude_regexes,
            exclude_git_repos=args.exclude_git_repos,
        )
    )
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

    if args.pdf_preview_page < 1:
        print("--pdf-preview-page must be at least 1", file=sys.stderr)
        return 1

    if args.repair_pdf_if_needed and not shutil.which("qpdf"):
        print(
            "--repair-pdf-if-needed requires qpdf in PATH (install with: brew install qpdf)",
            file=sys.stderr,
        )
        return 1

    if args.repair_pdf_macos_pdfkit:
        if not shutil.which("swift"):
            print(
                "--repair-pdf-macos-pdfkit requires swift (install Xcode Command Line Tools)",
                file=sys.stderr,
            )
            return 1
        pdfkit_swift = Path(__file__).resolve().with_name("macos_pdf_resave.swift")
        if not pdfkit_swift.is_file():
            print(
                f"--repair-pdf-macos-pdfkit requires {pdfkit_swift.name} next to the script "
                f"(expected at {pdfkit_swift})",
                file=sys.stderr,
            )
            return 1

    show_progress = sys.stderr.isatty() and not args.no_progress

    shutdown = Event()
    old_sigint, old_sigterm = _install_graceful_interrupt_handlers(shutdown)
    audit_handle = None
    write_lock = Lock()
    interrupted = False
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
                if shutdown.is_set():
                    interrupted = True
                    break
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
                    pdf_preview_page=args.pdf_preview_page,
                    repair_pdf_if_needed=args.repair_pdf_if_needed,
                    pdf_repair_backup_suffix=args.pdf_repair_backup_suffix,
                    repair_pdf_macos_pdfkit=args.repair_pdf_macos_pdfkit,
                    pdf_pdfkit_repair_backup_suffix=args.pdf_pdfkit_repair_backup_suffix,
                    audit_handle=audit_handle,
                    write_lock=write_lock,
                )
                report_outcome(outcome, dry_run=args.dry_run)
        else:
            executor = ThreadPoolExecutor(max_workers=args.workers)
            try:
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
                        pdf_preview_page=args.pdf_preview_page,
                        repair_pdf_if_needed=args.repair_pdf_if_needed,
                        pdf_repair_backup_suffix=args.pdf_repair_backup_suffix,
                        repair_pdf_macos_pdfkit=args.repair_pdf_macos_pdfkit,
                        pdf_pdfkit_repair_backup_suffix=args.pdf_pdfkit_repair_backup_suffix,
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
                        interrupted = _drain_executor_futures(
                            futures,
                            shutdown,
                            dry_run=args.dry_run,
                            pbar=pbar,
                        )
                else:
                    interrupted = _drain_executor_futures(
                        futures,
                        shutdown,
                        dry_run=args.dry_run,
                        pbar=None,
                    )
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
    finally:
        _restore_graceful_interrupt_handlers(old_sigint, old_sigterm)
        if audit_handle is not None:
            audit_handle.close()

    if interrupted:
        print("Stopped early by interrupt.", file=sys.stderr)
        return 130

    return 0


def audit_dict_for_outcome(outcome: ProcessOutcome) -> dict[str, object]:
    """Build one JSON object for the audit log (success and failure rows)."""
    base_path = str(outcome.original_path)
    if outcome.skipped_reason:
        row: dict[str, object] = {
            "status": "skipped",
            "original_path": base_path,
            "skipped_reason": outcome.skipped_reason,
        }
        if outcome.pdf_repair_status:
            row["pdf_repair_status"] = outcome.pdf_repair_status
        return row

    assert outcome.renamed_path is not None
    assert outcome.result is not None
    row = {
        "status": "ok",
        "original_path": base_path,
        "renamed_path": str(outcome.renamed_path),
        "summary": outcome.result.summary,
        "title": outcome.result.title,
        "source_kind": outcome.result.source_kind,
        "metadata": outcome.result.metadata,
    }
    if outcome.pdf_repair_status:
        row["pdf_repair_status"] = outcome.pdf_repair_status
    if outcome.pdf_status:
        row["pdf_metadata_status"] = outcome.pdf_status
    return row


def write_audit_line(
    audit_handle,
    outcome: ProcessOutcome,
    dry_run: bool,
) -> None:
    """Append one JSONL record when running for real with an audit log."""
    if audit_handle is None or dry_run:
        return
    audit_handle.write(
        json.dumps(audit_dict_for_outcome(outcome), ensure_ascii=False) + "\n"
    )
    audit_handle.flush()


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
                if row.get("status") == "skipped":
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


def _path_matches_exclude_glob(rel_posix: str, basename: str, pattern: str) -> bool:
    if "/" in pattern:
        return fnmatch.fnmatch(rel_posix, pattern)
    return fnmatch.fnmatch(basename, pattern)


def _path_matches_exclude_regex(rel_posix: str, pattern: re.Pattern[str]) -> bool:
    return pattern.search(rel_posix) is not None


def _is_excluded_rel_path(
    rel_posix: str,
    basename: str,
    exclude_globs: Sequence[str],
    exclude_regexes: Sequence[re.Pattern[str]],
) -> bool:
    for g in exclude_globs:
        if _path_matches_exclude_glob(rel_posix, basename, g):
            return True
    for rx in exclude_regexes:
        if _path_matches_exclude_regex(rel_posix, rx):
            return True
    return False


def _is_git_repo_root(dir_path: Path) -> bool:
    """True if dir_path looks like a Git working tree (nested repo), not necessarily the scan root."""
    try:
        return (dir_path / ".git").exists()
    except OSError:
        return False


def iter_files(
    root: Path,
    include_hidden: bool,
    *,
    exclude_globs: Sequence[str] = (),
    exclude_regexes: Sequence[re.Pattern[str]] = (),
    exclude_git_repos: bool = False,
) -> Iterable[Path]:
    root = root.resolve()
    exclude_globs = tuple(exclude_globs)
    exclude_regexes = tuple(exclude_regexes)
    for current_root, dirnames, filenames in os.walk(root, topdown=True):
        current_path = Path(current_root)
        kept_dirs: list[str] = []
        for d in dirnames:
            if not include_hidden and d.startswith("."):
                continue
            child = current_path / d
            if exclude_git_repos and _is_git_repo_root(child):
                continue
            rel = child.relative_to(root).as_posix()
            if _is_excluded_rel_path(rel, d, exclude_globs, exclude_regexes):
                continue
            kept_dirs.append(d)
        dirnames[:] = kept_dirs

        for filename in filenames:
            if not include_hidden and filename.startswith("."):
                continue
            if filename.lower() in SKIP_FILENAMES:
                continue
            rel = (current_path / filename).relative_to(root).as_posix()
            if _is_excluded_rel_path(rel, filename, exclude_globs, exclude_regexes):
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
    pdf_preview_page: int = 1,
    repair_pdf_if_needed: bool = False,
    pdf_repair_backup_suffix: str = ".qpdf-repair-backup.pdf",
    repair_pdf_macos_pdfkit: bool = False,
    pdf_pdfkit_repair_backup_suffix: str = ".pdfkit-repair-backup.pdf",
) -> ProcessOutcome:
    pdf_repair_status: str | None = None
    if (
        (repair_pdf_if_needed or repair_pdf_macos_pdfkit)
        and file_path.suffix.lower() in PDF_EXTENSIONS
    ):
        try:
            pdf_repair_status = maybe_repair_pdf_if_needed(
                file_path=file_path,
                dry_run=dry_run,
                repair_backup_suffix=pdf_repair_backup_suffix,
                pdfkit_backup_suffix=pdf_pdfkit_repair_backup_suffix,
                use_qpdf=repair_pdf_if_needed,
                use_macos_pdfkit=repair_pdf_macos_pdfkit,
            )
        except RuntimeError as exc:
            outcome = ProcessOutcome(
                original_path=file_path,
                renamed_path=None,
                result=None,
                skipped_reason=f"PDF repair failed: {exc}",
            )
            write_audit_line(audit_handle, outcome, dry_run)
            return outcome

    try:
        result = analyze_file(
            file_path=file_path,
            text_model=text_model,
            vision_model=vision_model,
            ollama_url=ollama_url,
            backend=backend,
            pdf_preview_page=pdf_preview_page,
        )
    except Exception as exc:  # noqa: BLE001
        outcome = ProcessOutcome(
            original_path=file_path,
            renamed_path=None,
            result=None,
            skipped_reason=str(exc),
            pdf_repair_status=pdf_repair_status,
        )
        write_audit_line(audit_handle, outcome, dry_run)
        return outcome

    safe_title = sanitize_title(result.title)
    if not safe_title:
        outcome = ProcessOutcome(
            original_path=file_path,
            renamed_path=None,
            result=result,
            skipped_reason="model returned an empty title",
            pdf_repair_status=pdf_repair_status,
        )
        write_audit_line(audit_handle, outcome, dry_run)
        return outcome

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

            outcome = ProcessOutcome(
                original_path=file_path,
                renamed_path=destination,
                result=result,
                pdf_status=pdf_status,
                pdf_repair_status=pdf_repair_status,
            )
            write_audit_line(audit_handle, outcome, dry_run)
        else:
            outcome = ProcessOutcome(
                original_path=file_path,
                renamed_path=destination,
                result=result,
                pdf_status=None,
                pdf_repair_status=pdf_repair_status,
            )

    return outcome


def report_outcome(outcome: ProcessOutcome, dry_run: bool) -> None:
    # Use tqdm.write so messages scroll above an active tqdm bar instead of
    # interleaving with stderr redraws (bar uses sys.stderr; logs use stdout/stderr).
    if outcome.skipped_reason:
        tqdm.write(
            f"SKIP {outcome.original_path.name}: {outcome.skipped_reason}",
            file=sys.stderr,
        )
        return

    assert outcome.renamed_path is not None
    assert outcome.result is not None

    action = "WOULD RENAME" if dry_run else "RENAMED"
    tqdm.write(f"{action}: {outcome.original_path.name} -> {outcome.renamed_path.name}")
    tqdm.write(f"  Summary: {outcome.result.summary}")
    metadata_preview = format_metadata_preview(outcome.result.metadata)
    if metadata_preview:
        tqdm.write(f"  Metadata: {metadata_preview}")
    if outcome.pdf_repair_status:
        tqdm.write(f"  PDF repair: {outcome.pdf_repair_status}")
    if outcome.pdf_status:
        tqdm.write(f"  PDF metadata: {outcome.pdf_status}")


def analyze_file(
    file_path: Path,
    text_model: str,
    vision_model: str,
    ollama_url: str,
    backend: str,
    pdf_preview_page: int = 1,
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
        preview_path = render_pdf_preview(file_path, page=pdf_preview_page)
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


def render_pdf_preview(file_path: Path, page: int = 1) -> Path:
    if page < 1:
        raise ValueError("page must be at least 1")

    if page > 1:
        swift_path = Path(__file__).with_name("macos_pdf_page_render.swift")
        if not swift_path.exists():
            raise RuntimeError(
                "macos_pdf_page_render.swift not found (needed for --pdf-preview-page > 1)"
            )
        if not shutil.which("swift"):
            raise RuntimeError("swift is required for --pdf-preview-page > 1")
        preview_out = Path(tempfile.mkstemp(suffix=".png")[1])
        try:
            completed = subprocess.run(
                [
                    "swift",
                    str(swift_path),
                    str(file_path),
                    str(page),
                    str(preview_out),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as exc:
            preview_out.unlink(missing_ok=True)
            raise RuntimeError(f"unable to render PDF page {page}: {exc}") from exc
        if completed.returncode != 0:
            preview_out.unlink(missing_ok=True)
            detail = (completed.stderr or completed.stdout or "").strip()
            raise RuntimeError(
                f"unable to render PDF page {page}: {detail[:400] if detail else 'swift failed'}"
            )
        return preview_out

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
        "Read the file content and produce a title, summary, and searchable metadata.\n\n"
        f"{DOCUMENT_ANALYSIS_PROMPT_RULES}\n\n"
        "Additional rules for this source:\n"
        "- Do not include the original numeric filename in the title.\n"
        "- If the document is ambiguous, choose the best factual title and metadata you can.\n\n"
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
        "Look at the image and infer the most accurate title, summary, and searchable metadata you can.\n\n"
        f"{DOCUMENT_ANALYSIS_PROMPT_RULES}\n\n"
        "If text in the image is partly unreadable, keep the title shorter and more conservative, "
        "but still token-rich (document type, year, clearest organization or person names). "
        "Prefer empty or partial metadata fields over guessing. Otherwise use the visible clues and stay conservative.\n\n"
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


def pdf_structure_likely_broken_for_exiftool(file_path: Path) -> bool:
    """Return True when the PDF is likely to fail exiftool (and similar tools), e.g. bad xref."""
    if shutil.which("exiftool"):
        completed = subprocess.run(
            ["exiftool", "-m", "-q", "-q", "-s", "-s", "-PDF:Version", str(file_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return True
    elif shutil.which("qpdf"):
        completed = subprocess.run(
            ["qpdf", "--check", str(file_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return True
    return False


def repair_pdf_with_qpdf(file_path: Path, backup_suffix: str) -> Path:
    """Rewrite PDF with qpdf in place. Returns the backup path. Restores the file on failure."""
    if not shutil.which("qpdf"):
        raise RuntimeError("qpdf is not available")

    backup_path = build_pdf_backup_path(file_path, backup_suffix)
    if backup_path.exists():
        raise RuntimeError(
            f"repair backup already exists at {backup_path}; move it or choose a different "
            "--pdf-repair-backup-suffix"
        )
    shutil.copy2(file_path, backup_path)
    fd, tmp_name = tempfile.mkstemp(suffix=".pdf", prefix=".qpdf-", dir=file_path.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        completed = subprocess.run(
            ["qpdf", str(file_path), str(tmp_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(detail[:500] if detail else "qpdf failed")
        if not tmp_path.is_file() or tmp_path.stat().st_size < 16:
            raise RuntimeError("qpdf produced empty or invalid output")
        if shutil.which("exiftool"):
            verify = subprocess.run(
                ["exiftool", "-m", "-q", "-q", "-s", "-s", "-PDF:Version", str(tmp_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            if verify.returncode != 0:
                raise RuntimeError("repaired PDF still fails exiftool read check")
        os.replace(tmp_path, file_path)
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        if backup_path.exists():
            shutil.copy2(backup_path, file_path)
        raise
    return backup_path


def pdfkit_resave_swift_path() -> Path:
    return Path(__file__).resolve().with_name("macos_pdf_resave.swift")


def repair_pdf_with_macos_pdfkit(file_path: Path, backup_suffix: str) -> Path:
    """Resave PDF via PDFKit (Preview-class rewrite). Returns backup path; restores file on failure."""
    swift = pdfkit_resave_swift_path()
    if not swift.is_file():
        raise RuntimeError(f"{swift.name} not found (needed for PDFKit repair)")
    if not shutil.which("swift"):
        raise RuntimeError("swift is required for PDFKit PDF repair")

    backup_path = build_pdf_backup_path(file_path, backup_suffix)
    if backup_path.exists():
        raise RuntimeError(
            f"PDFKit repair backup already exists at {backup_path}; move it or choose a different "
            "--pdf-pdfkit-repair-backup-suffix"
        )
    shutil.copy2(file_path, backup_path)
    fd, tmp_name = tempfile.mkstemp(suffix=".pdf", prefix=".pdfkit-", dir=file_path.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        completed = subprocess.run(
            ["swift", str(swift), str(file_path), str(tmp_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(detail[:500] if detail else "swift PDFKit resave failed")
        if not tmp_path.is_file() or tmp_path.stat().st_size < 16:
            raise RuntimeError("PDFKit produced empty or invalid output")
        if shutil.which("exiftool"):
            verify = subprocess.run(
                ["exiftool", "-m", "-q", "-q", "-s", "-s", "-PDF:Version", str(tmp_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            if verify.returncode != 0:
                raise RuntimeError("PDFKit-resaved PDF still fails exiftool read check")
        os.replace(tmp_path, file_path)
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        if backup_path.exists():
            shutil.copy2(backup_path, file_path)
        raise
    return backup_path


def maybe_repair_pdf_if_needed(
    file_path: Path,
    *,
    dry_run: bool,
    repair_backup_suffix: str,
    pdfkit_backup_suffix: str = ".pdfkit-repair-backup.pdf",
    use_qpdf: bool = True,
    use_macos_pdfkit: bool = False,
) -> str | None:
    """
    If the PDF appears unreadable by exiftool (or fails qpdf --check when exiftool is absent),
    optionally rewrite with qpdf and/or resave with macOS PDFKit. In dry-run mode, only reports.
    """
    state = inspect_pdf_safety(file_path)
    if state["is_encrypted"]:
        return "skipped (encrypted PDF)"
    if not pdf_structure_likely_broken_for_exiftool(file_path):
        return None
    if not use_qpdf and not use_macos_pdfkit:
        return None

    labels: list[str] = []
    if use_qpdf:
        labels.append("qpdf")
    if use_macos_pdfkit:
        labels.append("macOS PDFKit")
    if dry_run:
        return f"would repair ({' then '.join(labels)})"

    steps: list[str] = []
    if use_qpdf:
        repair_pdf_with_qpdf(file_path, repair_backup_suffix)
        steps.append("qpdf")
    if use_macos_pdfkit:
        if not use_qpdf or pdf_structure_likely_broken_for_exiftool(file_path):
            repair_pdf_with_macos_pdfkit(file_path, pdfkit_backup_suffix)
            steps.append("macOS PDFKit")
    return f"repaired with {' then '.join(steps)} (backups beside original)"


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


def strip_trailing_extension_from_title(title: str, extension: str) -> str:
    """Drop trailing extension tokens that duplicate the source file's suffix.

    The model sometimes includes ``.pdf`` (etc.) in the title; ``unique_destination``
    then appends the real extension, producing ``name.pdf.pdf``.
    """
    if not extension:
        return title
    ext_lower = extension.lower()
    base = title
    while len(base) >= len(extension) and base.lower().endswith(ext_lower):
        base = base[: -len(extension)].rstrip(" ._-")
    return base


def unique_destination(source: Path, title: str) -> Path:
    extension = source.suffix
    stem = strip_trailing_extension_from_title(title, extension)
    candidate = source.with_name(f"{stem}{extension}")
    counter = 2
    while candidate.exists() and candidate != source:
        candidate = source.with_name(f"{stem} {counter}{extension}")
        counter += 1
    return candidate


if __name__ == "__main__":
    raise SystemExit(main())
