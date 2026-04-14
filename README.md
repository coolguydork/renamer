# Ollama Document Renamer

This script scans a directory, asks your local Ollama instance to summarize each file, extracts structured searchable metadata, and renames the file to that title.

It is designed for messy document archives where filenames are numeric or otherwise unhelpful.

## What it can handle

- Plain text files such as `.txt`, `.md`, `.csv`, `.json`, `.xml`
- Office-like documents that `textutil` can convert on macOS, such as `.docx`, `.doc`, `.rtf`, `.html`
- PDFs using macOS metadata text when available
- Scanned PDFs and images using built-in macOS OCR via Vision
- Images and scanned PDFs by sending an image preview to a vision-capable Ollama model

For every supported file, the model now attempts to extract:

- A concise filename-safe title
- A short summary
- Structured metadata such as document type, people, organizations, locations, dates, identifiers, keywords, and language

## Requirements

- macOS
- Python 3
- A running local Ollama server, usually via `ollama serve`
- A model for text analysis, and ideally a vision-capable model for scanned pages
- `swift` available on macOS for the built-in OCR helper

Examples of possible models:

- Text: `llama3.2`, `mistral`, `qwen2.5`
- Vision: `llava`, `llama3.2-vision`

Use `ollama list` to see which models are actually installed on your machine. The script will now report installed model names if you pass one that is missing.
The HTTP integration uses Ollama's current `/api/chat` and `/api/tags` endpoints, with `http://127.0.0.1:11434` as the default base URL.

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

For PDFs, the script can also write a conservative set of native metadata fields in both PDF Info and XMP. This mode is designed to prioritize safety:

- It only attempts PDF metadata writes for `.pdf` files
- It skips PDFs that appear encrypted
- It skips PDFs that appear digitally signed
- It creates a backup copy before any metadata update
- It can optionally validate the PDF after writing and restore the backup automatically if validation fails

```bash
python3 /path/to/ollama_document_renamer.py /path/to/archive \
  --model llama3:latest \
  --vision-model llava \
  --write-pdf-metadata \
  --validate-pdf-after-write
```

## Command-line reference

All options match `ollama_document_renamer.py` (`python3 -m` is not used; run the file or the `ollama-document-renamer` entry point). Run with `--help` for the same text argparse prints.

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

### Audit log

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--audit-log` | `rename_audit.jsonl` | JSONL path for original path, new path, summary, and structured metadata. |
| `--overwrite` | off | Allow replacing an existing audit log; starts a new log and **ignores `--resume`**. |
| `--resume` | off | Skip files already recorded in the audit log (by renamed path); append new lines. Use with the same `directory` and `--audit-log` after an interrupted run. |

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

## Notes

- The script preserves file extensions.
- Analysis can now run in parallel across files, but the final rename and metadata-write step is still coordinated to avoid filename collisions and audit-log corruption.
- If a generated filename already exists, the script appends ` 2`, ` 3`, and so on.
- Finder comments are a practical cross-file search target on macOS even when file formats expose very different internal metadata fields.
- PDF metadata mode uses `exiftool` to fill a conservative set of Info and XMP fields, including title, subject/description, keywords, and a small set of corresponding XMP labels.
- Each PDF metadata update creates a backup copy next to the PDF by default, using the suffix `.metadata-backup.pdf`.
- Validation mode checks that the edited PDF can still be read by `exiftool`, recognized by `mdls`, and rendered by macOS Quick Look before considering the write successful.
- For scanned PDFs, the preview is usually based on the first page thumbnail, so a vision model works best when the first page is representative.
- If your PDFs already contain embedded text, `--backend cli` can be enough even without a vision model.
- Even without a vision model, scanned PDFs may still work because the script first tries local macOS OCR on a rendered preview.
