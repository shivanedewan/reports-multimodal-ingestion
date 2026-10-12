# Reports multimodal ingestion

Python ingestion for `reports_main_safe` and `reports_attachment_safe`, producing a versioned Elasticsearch chunk index using the two existing EmbeddingGemma 2 vLLM servers. Source indexes are read only. No LLM answers are generated.

The development workspace cannot reach the deployment network. Tests use synthetic files and mocked services. Run deployment preflight on the ingestion VM before sending records.

## Install

Use Python 3.11+ on the ingestion VM (tested locally with Python 3.14). Keep runtime state/cache on reliable local disk, outside OneDrive/NFS. Source files may be read from mounted storage.

```bash
python -m venv .venv
# Linux: source .venv/bin/activate
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test,pdf,archives]"
```

`requirements.lock.txt` records the tested core dependency versions. The `pdf` and `archives` extras have separate optional dependencies; select and validate their versions on your deployment platform. For ordinary operation, `[test]` is unnecessary.

External dependencies:

| Tool | When needed |
| --- | --- |
| FFmpeg and FFprobe on PATH | All audio codecs; optional video decoding |
| PyMuPDF (`pdf` extra) | PDF page rendering/native visual embeddings; pypdf still extracts text without it |
| Apache Tika server | Legacy DOC/XLS/PPT, MSG/mail stores, proprietary document formats without usable ES Text |
| LibreOffice executable | Optional isolated Office-to-text conversion fallback; Tika is preferred for spreadsheets/presentations |
| Tesseract | Optional OCR text from image/PDF pages; native image vectors work without OCR |
| rarfile plus supported backend | Optional bounded RAR member reads |

Configure `tika_url`, e.g. `http://127.0.0.1:9998`, to enable Tika. Provision a current official Tika server on your infrastructure with a matching request timeout. This project calls its `/tika` text-extraction endpoint; it does not download or start Java services automatically. Set `libreoffice`/`tesseract` to installed executable paths when enabling them.

## Configure

Copy `config.example.json` to `config.local.json` and edit it. Local configuration is ignored by Git.

Existing deployment addresses are included:

- Elasticsearch: `http://192.168.10.236:9200`, username `elastic`.
- vLLM: `http://192.168.10.210:9081` and `http://192.168.10.210:9082`.

Confirm HTTP/HTTPS and certificate configuration on deployment. Passwords/API keys come from environment variables, never the JSON file. Use `ES_PASSWORD` for basic authentication, or `ES_API_KEY` for API-key authentication. If both are set, the API key takes precedence. `VLLM_API_KEY` is optional for protected servers.

PowerShell:

```powershell
$secret = Read-Host 'Elasticsearch password' -AsSecureString
$env:ES_PASSWORD = [System.Net.NetworkCredential]::new('', $secret).Password
```

Bash:

```bash
read -rsp 'Elasticsearch password: ' ES_PASSWORD; echo
export ES_PASSWORD
```

Required deployment edits:

1. Set `embedding.model` to the exact name advertised by both `/v1/models` endpoints.
2. Set `model_revision` and `tokenizer_revision` to immutable deployed revisions, not `main/latest`. Match the actual server model/processor revision; this client cannot infer it from a served alias.
3. Copy that model revision's `tokenizer.json` to `assets/tokenizer.json`, or point `tokenizer_path` at it. No weights are loaded in the ingestion client. The tokenizer file hash participates in the build contract.
4. Set `required_mounts` to your actual source-storage roots. This prevents an inaccessible share being mistaken for thousands of individually missing files. On Linux, configure `path_prefixes` for Windows/UNC SystemPath roots when needed; the matched suffix's separators are converted.
5. Set `runtime.directory/state_path/temp_path/text_cache_path` and disk limits for production capacity. The 20 GiB normalized-text cache budget is an initial bound, not a six-million-record capacity estimate. Set a larger budget or `0` for unlimited-by-policy with free-disk checks. Cache entries are not silently evicted while referenced by chunks.
6. Confirm `parent_lookup_field` against the source mapping (`DocId` for numeric/keyword fields, often `DocId.keyword` for text fields). Parent lookup uses this field, never assumes DocId equals ES `_id`.
7. Tune shards/replicas and indexed-vector options for your exact Elasticsearch release. Default `hnsw` is a conservative explicit compatibility choice; quantized options require testing and a new build configuration.
8. Review `pdf_visual_mode`: `scanned` embeds scan pages; `mixed` also represents image-containing pages; `all` renders every page. Suppressed visual coverage on text pages is logged.

All ingestion commands run preflight: pinned tokenizer, enabled audio tools, mounts/disk, Elasticsearch version/mappings and synthetic text/image/audio requests to BOTH vLLM servers. Missing optional codecs/parser tools are still per-source logged failures/fallbacks.

### vLLM contract

Text uses `/v1/embeddings` with a list of document-prefixed inputs. Defaults are 16 inputs per endpoint batch, with 24,000-token and 16-MiB client budgets. No client model weights or GPUs are needed.

Media defaults to `/v1/embeddings` with one chat-style `messages` sequence per sample. Image uses `image_url` with a base64 data URI; audio uses `input_audio` with WAV base64. Several samples are submitted concurrently (`media_concurrency=4`) so vLLM can continuously batch them. They are **not** put into one message, which would produce a combined embedding.

The installed vLLM pooling implementation and model chat template must support this schema. If its documented schema differs, configure `media_path` and `media_extra_body`, or adapt `EmbeddingClient.single_media`; preflight rejects incompatible endpoints before corpus ingestion. `cohere_images` enables batched image requests to `/v2/embed` when the deployed server supports them. Audio still uses independent message requests. No automatic modality downgrade is performed after a failed model preflight.

Image batch target is 8 and audio batch target is 4 initially; raise toward 10–20 and raise media concurrency after observing server/payload limits. Both servers have independent scheduler threads. Text chunks target 1,000 tokens, overlap up to 150, and cap at 1,400 total input tokens including title/prefix. Audio defaults to 60 seconds with a 50-second stride, decoded to 16 kHz mono. Models must emit finite, nonzero 768d sequence embeddings, normalized by the client.

## Commands

Global `--config` goes **before** the subcommand.

```bash
python -m reports_ingest --config config.local.json capabilities
python -m reports_ingest --config config.local.json preflight --local-only
python -m reports_ingest --config config.local.json preflight
python -m reports_ingest --config config.local.json inventory
python -m reports_ingest --config config.local.json audit --samples-per-type 3
python -m reports_ingest --config config.local.json ingest --create-index --limit 2000
python -m reports_ingest --config config.local.json status
python -m reports_ingest --config config.local.json resume
python -m reports_ingest --config config.local.json retry-failures
python -m reports_ingest --config config.local.json publish
```

`inventory` streams all source records and emits exhaustive extension counts without requiring an aggregatable extension field. `--limit` labels its output as a sample. `audit` reads a stratified sample by index/extension/Text availability/Branch and exercises extraction without embedding or target writes. It scans the corpus to find those strata; it is not a quick count-only command.

`--limit` counts newly scheduled/retried sources, not just read hits. A limited pilot cannot be published as a full build. `resume` replays pending jobs, then starts fresh source PITs, skipping unchanged completed sources from durable state. Expired PIT cursors are not reused with a new PIT. `retry-failures` only retries retained partial/failed/pending source payloads and preserves a previous full-enumeration marker.

Publication requires the original matching state database/build ID, a completed full enumeration, no pending jobs, no unobserved/disappeared previous sources, and target chunk count matching durable acknowledgements. Partial/failed sources block by default; `publish --allow-partial` is an explicit operator choice after reviewing CSVs. Skipped sources with no content are reconciled but intentionally absent from the target. The alias changes in one ES alias operation after refresh. Ingestion into an already published target is refused.

Offline extraction inspection, requiring no Elasticsearch/model connection:

```bash
python -m reports_ingest --config config.example.json inspect-file reference.txt
python -m reports_ingest --config config.local.json inspect-file /mounted/path/legacy.zip
python -m reports_ingest --config config.example.json mapping
python -m pytest -q
```

`inspect-file` reports modality, character/media-byte counts and source locators without printing document contents. `mapping` prints the explicit target template; normal index creation should use `ingest --create-index` to attach the state/build identity.

## Formats and fallback policy

- Missing physical file + usable ES Text: text-only vector, `UsedTextFallback=true`, original unavailable, no downloadable preview.
- Missing physical file + empty/unusable Text: logged skip, no placeholder vector.
- Valid ordinary prose/Office with good ES Text: use it directly. PDF/sheets/slides use file structure for navigation with Text fallback on extraction failure.
- Images/audio: actual media vectors, with separate associated-text vectors if meaningful Text exists. Arbitrary Text is not assumed to be a timestamped transcript.
- DOCX/ODT/ODF packages have native text extraction. XLSX uses sheet/row-aware extraction. EPUB has section text. Legacy binary documents require configured Tika/conversion or ES Text fallback.
- PDFs preserve page anchors; image TIFFs preserve page/frame identity. Optional OCR is a distinct text representation. Region detection, native temporal-video embeddings and ASR transcription generation are not included; optional video uses sampled-frame image vectors and segmented audio.
- ZIP/TAR/GZIP/BZIP2/XZ: bounded member processing and nested locators. Bad/encrypted/unsupported members do not stop siblings. RAR requires its adapter/backend. Safe streaming 7z expansion is deliberately unavailable; valid ES Text still rescues the source, otherwise it is logged and skipped. Executables/unknown binaries are not executed.
- Office embedded images and EML MIME attachments are currently coverage warnings, rather than silently advertised as processed. Use source attachments indexed separately or add dedicated adapters for that content. ODS/ODP extraction currently produces package text, without fabricated sheet/slide positions.

Default archive policies: depth 3, 10,000 members, 2 GiB aggregate actual expanded bytes, 100 MiB per generic document, ratio 200. Media has a separate 8-GiB source/member byte cap, still constrained by aggregate archive expansion. ZIP traversal/absolute paths/escaping links are rejected. Limits and omitted coverage are logged. Original SystemPath always points to the source container/file; temporary member paths are never persisted as source locators.

## Recovery, metadata and logs

The pipeline bounds queued records by both count and estimated bytes, queued embedding units by count/token/payload budgets, and target bulk requests by count/JSON bytes. Source readers shrink ES page size if a response exceeds the decoded-byte budget. Extraction workers are persistent subprocesses; crash/timeouts isolate the source and use Text fallback where possible. POSIX workers also use configured address-space limits; Windows retains time/byte/pixel/process isolation but lacks POSIX rlimits.

Completed text is stored compressed by content hash for stable snippet offsets. Embedded source chunks spool to temporary local files until extraction/embedding coverage is known, then bulk-index with final coverage flags. Disk pressure stops ingestion with pending state, rather than deleting referenced cache or dropping work. Temporary spools are regenerated on resume, with deterministic chunk IDs.

IDs use source index + original ES `_id`, generation, member/representation/location and content; successful members retain identity when another member recovers. DocId/ParentDocId/RootDocId remain available for report-family grouping. FileKey distinguishes virtual archive members. Original ES `_id` is retained because it can differ from DocId.

Children inherit Branch/ReportType/DocumentDate only when missing and resolved from their parent. `MetadataInheritedFields` records those choices. ACLs are copied from the source and never inferred or unioned with parent ACLs. The retrieval service must enforce your authorization policy before retrieval and previews. Arbitrary extra source metadata is preserved in a nonindexed object; only explicit fields are indexed.

Runtime outputs:

| Output | Purpose |
| --- | --- |
| `state.sqlite` | Jobs, fingerprints, completion counts, build ID, enumeration marker; SQLite WAL, one ingestion process |
| `ingestion_errors.csv` | DocId/index, source/member/chunk locator, stage/code/reason, retries, endpoint and disposition |
| `source_outcomes.csv` | Source-level complete/fallback/partial/skipped/failed outcomes |
| `file_type_inventory.csv` | Full or explicitly sampled format counts |
| `file_access_audit.csv` | Sample availability/parser coverage |
| `preflight.json` | Deployment mappings/counts and synthetic endpoint checks |
| `run-<id>.json` | Durable-state summary and fatal condition |
| `text_cache/` | Hash-addressed normalized text, compressed; references relative to this cache root |

CSV uses UTF-8 and real CSV quoting, with size-based rotation. It records recovered HTTP/bulk retries too. No full document text, request bodies, credentials or converter stderr are logged. Protect the logs as source paths/metadata can be private.

## Updates and operational limits

This release implements **resumable initial versioned builds**, not atomic live per-file updates. A changed previously enumerated source, model/tokenizer/processing contract or disappeared source requires a new target index and new state path; publish the new read alias after reconciliation. Existing published builds remain available during that work. DocumentDate is not treated as an update feed.

Default content fingerprints read source bytes to catch changes even when size/mtime are preserved. `source_fingerprint_mode=stat` reduces I/O but can miss such changes; use only under an appropriate immutable-file guarantee. Original payloads/derived texts still carry content hashes. Source records/files should be stable during a build; a PIT snapshots ES metadata, not external file storage.

Back up the state DB, WAL and normalized cache together using SQLite-aware backup or a stopped job. Keep the matching build ID; starting a different empty state DB against an existing populated target is rejected. Only one ingestion/publication process may own a state path. This is not a multi-VM distributed queue.

Permanent chunk/mapping failures mark their source partial/failed. Infrastructure/API-contract failures stop with pending work and a nonzero exit status. Both model services unavailable stops safely; it does not mark the entire corpus corrupt. Exit codes: 0 successful command, 2 configuration/service/processing blocker, 130 interrupted. Run summaries can still contain partial/skipped sources; publication policy is the final completeness check.

Source enumeration, field mappings, actual codecs and vLLM multimodal requests must be verified on deployment. No live throughput or retrieval-quality claims are made from mocked tests.
