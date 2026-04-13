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

## Useful flags

- `--dry-run`: preview renames without changing files
- `--workers`: process multiple files in parallel; start with `2` to `4` for mixed archives
- `--audit-log`: write a JSONL log of original names, new names, summaries, and structured metadata
- `--overwrite`: allow replacing an existing audit log
- `--include-hidden`: include hidden files and folders
- `--write-spotlight-comment`: store the summary and extracted metadata in the macOS Finder comment
- `--write-pdf-metadata`: write conservative native metadata into PDF Info and XMP fields for safe-to-edit PDFs
- `--pdf-backup-suffix`: suffix for the rollback copy created before PDF metadata writes
- `--validate-pdf-after-write`: validate PDFs after metadata updates and auto-restore the backup on validation failure
- `--delete-pdf-backup-on-success`: remove the backup after a successful metadata write and optional validation
- `--max-files 25`: test on a small subset first
- `--ollama-url`: point at a non-default Ollama base URL or API URL
- `--backend cli`: use the `ollama` command-line client instead of the HTTP API for text-only workloads

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
