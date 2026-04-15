# Ollama Document Renamer

This script scans a directory, asks your local Ollama instance to summarize each file, extracts structured searchable metadata, and renames the file to that title.

It is designed for messy document archives where filenames are numeric or otherwise unhelpful.

## What it can handle

- Plain text files such as `.txt`, `.md`, `.csv`, `.json`, `.xml`
- Office-like documents that `textutil` can convert on macOS, such as `.docx`, `.doc`, `.rtf`, `.html`
- PDFs using macOS metadata text when available
- Scanned PDFs and images using built-in macOS OCR via Vision
- Images and scanned PDFs by sending an image preview to a vision-capable Ollama model

For every supported file, the model attempts to extract:

- A concise filename-safe title
- A short summary
- Structured metadata such as document type, people, organizations, locations, dates, identifiers, keywords, and language

## Requirements

- **macOS**
- **Python 3.10+** (see `pyproject.toml`)
- A running local **Ollama** server, usually via `ollama serve`
- A model for text analysis, and ideally a vision-capable model for scanned pages
- **`swift`** on your PATH (Xcode Command Line Tools) for the bundled macOS helpers: OCR, PDF page rendering, optional PDFKit resave, and PDF preview when `--pdf-preview-page` is greater than `1`

Examples of possible models:

- Text: `llama3.2`, `mistral`, `qwen2.5`
- Vision: `llava`, `llama3.2-vision`

Use `ollama list` to see which models are actually installed on your machine. The script reports installed model names if you pass one that is missing.

The HTTP integration uses Ollama's current `/api/chat` and `/api/tags` endpoints, with `http://127.0.0.1:11434` as the default base URL.

### Optional command-line tools

| Tool | Used for |
| ---- | -------- |
| **`exiftool`** | Writing and validating PDF metadata (`--write-pdf-metadata`, validation) |
| **`qpdf`** | Rewriting damaged PDFs (`--repair-pdf-if-needed`) |
| **`xattr`** | Finder / Spotlight comments (`--write-spotlight-comment`) |
| **`mdls`**, **`qlmanage`** | Extra checks after PDF metadata writes (`--validate-pdf-after-write`) |

### Bundled Swift sources (next to `ollama_document_renamer.py`)

| File | Role |
| ---- | ---- |
| `macos_ocr.swift` | Vision OCR for images and PDF previews |
| `macos_pdf_page_render.swift` | Render a chosen PDF page when `--pdf-preview-page` is greater than `1` |
| `macos_pdf_resave.swift` | PDFKit resave when `--repair-pdf-macos-pdfkit` is enabled |

## Running the script

- From a checkout: `python3 /path/to/ollama_document_renamer.py …`
- After installing the package (for example `pip install .` or `pipx install .`): `ollama-document-renamer …`

The examples below use the `python3 …/ollama_document_renamer.py` form; the same flags work with `ollama-document-renamer`.

## Dry run first

```bash
python3 /path/to/ollama_document_renamer.py /path/to/archive \
  --model llama3:latest \
  --vision-model llava \
  --dry-run \
  --workers 4
```

## Rename files for real

```bash
python3 /path/to/ollama_document_renamer.py /path/to/archive \
  --model llama3:latest \
  --vision-model llava
```

## Add Spotlight-searchable comments

On macOS you can also write the generated summary and metadata into the file's Finder comment, which helps Spotlight index more than just the filename:

```bash
python3 /path/to/ollama_document_renamer.py /path/to/archive \
  --model llama3:latest \
  --vision-model llava \
  --write-spotlight-comment
```

## Add conservative PDF metadata

For PDFs, the script can write a conservative set of native metadata fields in both PDF Info and XMP. This mode prioritizes safety:

- Only attempts metadata writes for `.pdf` files
- Skips PDFs that appear encrypted
- Skips PDFs that appear digitally signed
- Creates a backup copy before any metadata update
- Can optionally validate the PDF after writing and restore the backup automatically if validation fails

```bash
python3 /path/to/ollama_document_renamer.py /path/to/archive \
  --model llama3:latest \
  --vision-model llava \
  --write-pdf-metadata \
  --validate-pdf-after-write
```

Metadata writes use **`exiftool`** when it is installed.

## PDF preview page and optional repair

- **`--pdf-preview-page`** — For OCR and vision preview, render a specific **1-based** page (default `1`). Use when the first page is a blank cover or not representative. Values greater than `1` use the bundled **`macos_pdf_page_render.swift`** (and **`qlmanage`** for page `1`).
- **`--repair-pdf-if-needed`** — Before analysis, detect PDFs that **`exiftool`** cannot read (for example bad xref tables) or that fail **`qpdf --check`**, then rewrite them in place with **`qpdf`**. Requires **`qpdf`** on your PATH (`brew install qpdf`). Creates a backup beside the file (`--pdf-repair-backup-suffix`, default `.qpdf-repair-backup.pdf`). Encrypted PDFs are skipped.
- **`--repair-pdf-macos-pdfkit`** — When a PDF is unreadable by **`exiftool`**, resave it with **PDFKit** (same macOS stack family as Preview for many save paths). Requires **`swift`** and **`macos_pdf_resave.swift`**. Use **alone** (no `qpdf`), or **with** `--repair-pdf-if-needed` so **`qpdf` runs first** and PDFKit runs **only if** the file is **still** unreadable. Backup: `--pdf-pdfkit-repair-backup-suffix` (default `.pdfkit-repair-backup.pdf`). Encrypted PDFs are skipped.

```bash
python3 /path/to/ollama_document_renamer.py /path/to/archive \
  --model llama3:latest \
  --vision-model llava \
  --pdf-preview-page 2 \
  --repair-pdf-if-needed \
  --repair-pdf-macos-pdfkit \
  --write-pdf-metadata
```

## Audit log (`rename_audit.jsonl`)

Each real run (not `--dry-run`) appends **one JSON object per file** to the audit log (default **`rename_audit.jsonl`**; override with **`--audit-log`**).

| `status` | Meaning |
| -------- | ------- |
| **`ok`** | Renamed successfully. Includes `original_path`, `renamed_path`, `summary`, `title`, `source_kind`, `metadata`. May include `pdf_repair_status` and `pdf_metadata_status` when those steps ran. |
| **`skipped`** | No rename: `skipped_reason` explains why (and optional `pdf_repair_status`). |

**`--resume`** skips inputs whose **successful** `renamed_path` already appears under the scan root; **`skipped`** lines do not block retries.

Filter examples with **`jq`**:

```bash
jq 'select(.status == "skipped")' rename_audit.jsonl
jq 'select(.status == "ok")' rename_audit.jsonl
```

## Command-line reference

All options match `ollama_document_renamer.py` (run the file or the `ollama-document-renamer` entry point). Run with **`--help`** for argparse’s full text.

### Positional

| Argument | Description |
| -------- | ----------- |
| `directory` | Directory to scan recursively. |

### Ollama and transport

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--model` | `llama3.2` | Model for text analysis. |
| `--vision-model` | *(same as `--model`)* | Model for image and PDF preview analysis; omit to reuse `--model`. |
| `--ollama-url` | `http://127.0.0.1:11434` | Ollama base URL or API URL. |
| `--backend` | `http` | `http` uses the HTTP API. `cli` uses `ollama run` (text only; no image payloads). `auto` tries HTTP first and falls back to the CLI if HTTP fails. Vision/image analysis still requires HTTP when the model needs images. |

### Run behavior

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--dry-run` | off | Show planned renames; do not rename files or write an audit log. |
| `--workers` | `1` | Number of files to process in parallel; try `2`–`4` on mixed archives if your machine and Ollama can keep up. |
| `--no-progress` | off | Disable the tqdm progress bar (it stays off automatically when stderr is not a TTY). |
| `--max-files` | *(none)* | Process at most the first *N* files after scanning and resume filtering. |

Press **Ctrl+C** once to request a graceful stop: in-flight work finishes, queued work is cancelled, exit code **130**.

### Audit log

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--audit-log` | `rename_audit.jsonl` | JSONL path (see [Audit log](#audit-log-rename_auditjsonl)). |
| `--overwrite` | off | Allow replacing an existing audit log; starts a new log and **ignores `--resume`**. |
| `--resume` | off | Skip files already successfully renamed per the log; append new lines. Same `directory` and `--audit-log` as the earlier run. |

### What gets scanned

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--include-hidden` | off | Include hidden files and directories (names starting with `.`). |
| `--exclude-glob` | *(repeatable)* | Skip directories (do not descend into them) and files matching a shell-style glob (`fnmatch`). Repeat for multiple patterns. If the pattern contains no `/`, it matches only the **final path component** (e.g. `node_modules`, `*.tmp`). If it contains `/`, it matches the **full path relative to the scan root**. |
| `--exclude-regex` | *(repeatable)* | Skip directories and files whose path **relative to the scan root** matches a Python regular expression (repeatable). Paths use `/` separators. Matching uses `re.search()` over that relative path. Invalid patterns exit with an error. |
| `--exclude-git-repos` | off | Do not descend into **subdirectories** that are Git repository roots (they contain a `.git` file or directory). The directory you pass as the scan root is still fully scanned. |

### macOS Finder and PDF metadata

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--write-spotlight-comment` | off | Write summary and extracted metadata into the macOS Finder/Spotlight comment. |
| `--write-pdf-metadata` | off | For safe PDFs only (not encrypted, not digitally signed), write conservative PDF Info and XMP metadata. |
| `--pdf-backup-suffix` | `.metadata-backup.pdf` | Suffix for the backup copy created beside the PDF before metadata writes. |
| `--validate-pdf-after-write` | off | After a metadata write, validate the PDF; restore the backup automatically if validation fails. |
| `--delete-pdf-backup-on-success` | off | After a successful metadata write (and optional validation), delete the backup file. |
| `--pdf-preview-page` | `1` | For PDF OCR and vision preview, render this **1-based** page (see [PDF preview page and optional repair](#pdf-preview-page-and-optional-repair)). |
| `--repair-pdf-if-needed` | off | Before analysis, rewrite unreadable PDFs with **qpdf** (requires `qpdf` in PATH). |
| `--pdf-repair-backup-suffix` | `.qpdf-repair-backup.pdf` | Backup suffix used before a qpdf repair. |
| `--repair-pdf-macos-pdfkit` | off | Resave unreadable PDFs with **PDFKit** (`swift` + `macos_pdf_resave.swift`). Combine with `--repair-pdf-if-needed` to try qpdf first, then PDFKit if still unreadable. |
| `--pdf-pdfkit-repair-backup-suffix` | `.pdfkit-repair-backup.pdf` | Backup suffix used before a PDFKit resave. |

## Notes

- The script preserves file extensions.
- Parallel **`--workers`** analyze files concurrently; renames and audit writes are coordinated so filenames and the JSONL log stay consistent.
- If a generated filename already exists, the script appends ` 2`, ` 3`, and so on.
- Finder comments are a practical cross-file search target on macOS even when file formats expose very different internal metadata fields.
- PDF metadata mode uses **`exiftool`** for Info and XMP fields (install **`exiftool`** if you use **`--write-pdf-metadata`**).
- PDF repair: **`--repair-pdf-if-needed`** uses **`qpdf`**; **`--repair-pdf-macos-pdfkit`** resaves via PDFKit (similar to re-saving in Preview). A PDFKit or qpdf rewrite can change the file bytes; **digitally signed PDFs** are skipped for metadata writes but **not** automatically skipped for repair—avoid repair flags on signed documents if signatures must stay valid.
- Each PDF metadata update creates a backup next to the PDF by default (`--pdf-backup-suffix`).
- Validation mode checks that the edited PDF can still be read by **`exiftool`**, recognized by **`mdls`**, and rendered by macOS Quick Look (**`qlmanage`**) before treating the write as successful.
- For scanned PDFs, the preview is usually based on the first page thumbnail unless you set **`--pdf-preview-page`**, so a vision model works best when the chosen page is representative.
- If your PDFs already contain embedded text, **`--backend cli`** can be enough even without a vision model.
- Even without a vision model, scanned PDFs may still work because the script tries local macOS OCR on a rendered preview first.
