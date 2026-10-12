# Multimodal ingestion and embedding implementation plan

Prepared 11 October 2026. This is the implementation specification requested before building the ingestion script. No live Elasticsearch inventory or ingestion has been run: the user confirmed this development environment is not connected to the real cluster and will move the project to production later. Deployment addresses are supplied; the exact ES/vLLM versions and multimodal API contract remain unverified.

## 1. Decisions and known inputs

- Read both `reports_main_safe` and `reports_attachment_safe` independently.
- Write one searchable Elasticsearch document per embedding unit into `reports_multimodal_chunks_v1`. Use a `reports_multimodal_chunks` read alias after validating the initial build.
- Retain the existing keyword-search pipeline. No answer generation is part of ingestion.
- `DocId` is globally unique, and each source record represents a physical file, as confirmed in `reference.txt`.
- `RootDocId = ParentDocId` for attachments with a valid parent reference; otherwise use the main record's own `DocId`. Missing or malformed attachment parent references are explicitly recorded, not silently fabricated.
- Elasticsearch deployment address: `http://192.168.10.236:9200`, username `elastic`. Keep the supplied password in a deployment environment variable/secret; do not put it in source, this plan, CSV logs or committed configuration. Confirm the transport scheme and certificate settings during deployment.
- Use the existing vLLM services at `http://192.168.10.210:9081` and `http://192.168.10.210:9082`. Confirm the transport scheme, served model name/revision, vLLM version, authentication configuration, and multimodal embedding request formats during deployment preflight. Do not infer modality support from a model being loaded.
- Keep 768-dimensional vectors initially. Dimension, model revision and preprocessing changes require a new index/build version; never mix incompatible embeddings in one vector field.
- A local durable state database and append-only CSV logs support the job. They do not require a fourth searchable Elasticsearch index. Put state on reliable local disk on the ingestion VM, not a synchronized OneDrive folder or shared network filesystem.

The third-index architecture remains appropriate: a file can produce text, visual-page and audio-segment representations, with common source identities supporting file and parent grouping. Separate modality indexes can be introduced later if operational measurements justify them; they are not necessary to start.

## 2. Read-only discovery before ingestion

Inspect the exact Elasticsearch version, source mappings, index settings and counts. Check field types, whether `_source` is enabled, date formats, analyzers, and whether attachment metadata/permissions differ from parent metadata. Do not assume the sample fields are the complete schema.

Produce `file_type_inventory.csv` with source-index name, raw and normalized extension, record count, nonempty-Text count and representative source IDs. Use exhaustive composite aggregation when the extension field is aggregatable; otherwise collect the inventory during a streamed scan. Include missing extensions. Do not use a top-N terms result as an exhaustive inventory.

Run a stratified file-access audit across both indexes, every observed extension, branches, ages and missing-Text states. Record detected MIME/signature, availability, approximate size, page count/duration where inexpensive, and parser outcome. Counts from this sample are estimates, not corpus-wide file-existence counts. Full ingestion determines actual availability for each record.

Normalize `.PDF`, `PDF`, whitespace, and compound endings such as `.tar.gz`. Use field extension, filename extension, and file signature together. Record disagreements. Distinguish OLE legacy Office files, OOXML/ODF packages, and ordinary ZIP archives; a `.docx` or `.odt` must not be treated as a generic archive just because it contains ZIP bytes.

The formats below are the planned coverage matrix, not a claim that all occur in the live indexes.

| Category | Candidate extensions | Processing policy |
| --- | --- | --- |
| Plain and structured text | txt, text, log, md, rst, csv, tsv, json, jsonl, xml, html, htm | Prefer usable ES Text; otherwise decode/parse file. Remove markup without erasing content. Preserve table context. |
| Word and legacy prose | doc, docx, docm, dot, dotx, dotm, rtf, odt, ott, fodt | Prefer usable ES Text; otherwise native text extraction/Tika. Isolated LibreOffice conversion is an optional fallback for damaged or unsupported layout extraction. |
| PDF and document scans | pdf | Preserve page anchors where obtainable. Text extraction first; OCR/visual page embedding for scans and mixed content. |
| Spreadsheets | xls, xlsx, xlsm, xlsb, ods, fods | Prefer sheet-aware extraction when the file exists; serialize header + row groups. Never embed an entire large workbook as one string. |
| Presentations | ppt, pptx, pptm, pps, ppsx, odp, fodp | Slide-aware extraction; retain slide number. Optional rendered-slide vectors for diagrams and visual content. |
| Images | jpg, jpeg, png, tif, tiff, bmp, webp, gif, heic, heif, jp2 | Decode with installed codec support; native image vector. TIFF pages and animated frames require explicit policies. OCR text is a separate representation. |
| Audio | wav, mp3, flac, m4a, aac, ogg, opus, wma, aif, aiff, amr, au | Probe/decode through FFmpeg, normalize and segment; native audio vectors. Optional transcript vectors when transcripts exist or ASR is configured. |
| Video | mp4, mkv, avi, mov, wmv, mpeg, mpg, webm, m4v, 3gp | Explicit optional adapter; extract audio and sampled visual segments. Inspect streams because container extension alone does not identify modality. |
| Archives/compression | zip, 7z, rar, tar, gz, tgz, bz2, xz, tar.gz, tar.bz2, tar.xz | Safe bounded expansion; recursively route supported members, retaining container/member provenance. Extra archive adapters depend on installed tools. |
| Mail and older document formats | eml, msg, mbox, pst, epub, mhtml, mht, wpd, sxw | Tika/native adapter where available; message/book-section identities. Large mail stores need a dedicated streaming adapter. |
| Other/unknown/binary | extensionless, mismatched formats, executables, databases, proprietary binaries | Signature detection and parser registry. Use usable ES Text if available; otherwise record unsupported/empty and skip. Do not execute binary content. |

Core delivery should cover the observed text/Office/PDF/image/audio types plus ZIP. Remaining formats are explicitly marked supported, dependency-required, or unsupported in the generated registry. Presence in this matrix does not guarantee every variant/codec/encrypted file is parseable.

Apache Tika provides broad Office, OpenDocument, mail and legacy format extraction; it does not transcribe spoken audio. Parser coverage and external dependencies must be pinned and checked against the selected release. [Tika formats](https://tika.apache.org/docs/4.0.x/formats.html). LibreOffice provides headless conversions where needed, with an isolated profile and a timeout per worker. [LibreOffice command-line documentation](https://help.libreoffice.org/latest/en-GB/text/shared/guide/start_parameters.html).

## 3. Content selection and missing-file policy

| File state | Usable ES Text | Outcome |
| --- | --- | --- |
| Present ordinary text/Office file | Yes | Embed ES Text by default; specialized page/sheet/slide extraction may replace it when needed for navigation/structure. |
| Present ordinary document | No | Extract file text; OCR scanned pages or use visual representations where appropriate. |
| Present image/audio/video | Either | Native media embedding from actual bytes; separately embed meaningful OCR/transcript Text when available. |
| Present archive | Either | Process actual members; use aggregate ES Text fallback only if no usable member content can be obtained. |
| Missing file | Yes | Embed Text only, set availability=missing, content source=es_text, fallback=true. Log warning; preview must say original unavailable. |
| Missing file | No | No chunk in target; log skipped_missing_file_no_text. |
| Corrupt/unreadable/unsupported file | Yes | Log the file failure and embed usable ES Text as a clearly identified text fallback. |
| Corrupt/unreadable/unsupported file | No | Log the reason and skip that source/member. |

Usable means actual nonempty meaningful text after conservative normalization, not merely an existing field. Reject extractor-error messages, NUL-filled/binary-looking garbage and known boilerplate-only extraction failures. Do not reject short meaningful names, numbers or phrases through an arbitrary minimum-length rule. Track quality signals and route doubtful text for audit.

Do not fetch a physical file to re-extract ordinary prose when satisfactory ES Text already exists merely for the sake of redoing work. Still check file availability to honor the requested fallback semantics. For PDF page navigation and spreadsheets/slides, specialized extraction is worth the additional I/O; retain the ES Text as fallback if extraction fails. Avoid indexing both copies of the same prose.

For image/audio records, Text cannot stand in for pixels or sound. A missing MP3 with Text produces Modality=text, SourceMediaType=audio and RepresentationKind=es_text_fallback. If it is a confirmed transcript, record that separately; never assume an arbitrary Text field is a transcript.

Filesystem errors must distinguish not-found from permission-denied, temporary network failure and inaccessible mount. If an entire share or mount is unavailable, pause/retry that source path family instead of incorrectly marking millions of files permanently missing. A metadata field indicating a path exists does not prove the file can be read.

## 4. Text chunking defaults

- Target: 1,000 model tokens of content.
- Overlap: up to 150 model tokens within the same logical section.
- Hard embedding input maximum: 1,400 tokens including title and document prefix.
- Use the exact tokenizer/revision used by EmbeddingGemma 2; no character-count approximation for final limits.
- Prefer paragraph, section, sentence, page, slide and table-row boundaries. Avoid mixing unrelated sections merely to fill the target.
- Pack nearby short paragraphs. Keep small meaningful documents as one chunk. Merge a short tail only when it fits the hard cap.
- For an oversized paragraph/row, split by token budget without dropping material. Preserve a repeated table header and sheet/row coordinates.
- Store offsets in the normalized text representation actually chunked. Do not claim those offsets refer to original PDF bytes or Word XML. Keep a versioned normalized-text cache or reproducible extraction artifact for precise text preview.
- `PageStart`/`PageEnd` are only populated when a real page map exists. ES Text alone generally supplies no trustworthy page numbers.

These are starting values for relevance testing, not model limits. Full model context is useful for inference flexibility; filling every chunk to 8K tokens can dilute short evidence and makes source navigation less precise.

Google documents an 8,192-token shared context and distinct retrieval-query/document prefixes. Apply the document prefix/title to text ingestion and the search-query prefix later at query time. No text task prefix is required for native media. [EmbeddingGemma 2 model card](https://ai.google.dev/gemma/docs/embeddinggemma/model_card_2).

## 5. Image, PDF and audio representations

Images: correct orientation, preserve aspect ratio, decode to supported RGB input, and pass through the pinned model processor. One vector per ordinary image initially. Multi-page TIFF gets one visual unit per page. Add tile/crop representations only after testing fine-detail queries; record each crop's bounding box and coordinate system. A whole-image vector cannot identify the exact object region automatically.

GIF: process a bounded, documented set of representative frames if animation matters; do not silently process thousands of frames. HEIC/JP2/other decoder failures become per-file failures with Text fallback where possible. Record original dimensions, frame/page and preprocessing version. Do not impose silent image decompression or page limits: every policy exclusion is counted and logged.

PDFs: text-heavy pages produce anchored text chunks. Scanned pages produce a native visual-page vector, plus OCR text chunks when OCR is configured. Mixed pages may need both. Embedded photographs/diagrams can produce additional image units when enabled. Record pages lacking OCR coverage; do not present native visual similarity as proof that a literal phrase was read. Rendering/OCR failures affect the failed page, not the whole corpus. Office prose does not require rendering every page; rendered slides/diagram-heavy documents are an optional visual pass.

Audio defaults: decode supported streams to 16 kHz mono and use 60-second windows with 10 seconds of overlap, hence a 50-second stride. Prefer nearby speech/silence boundaries without creating gaps. Retain absolute start/end seconds; avoid padding the last window with synthetic silence. Very short recordings remain one segment. Native acoustic/speech retrieval works without an ASR service; spoken-word exact search requires a transcript.

Google specifies audio at 25 tokens per second, so a 60-second audio input is about 1,500 audio tokens. Longer segments up to 90 seconds can be tested for contextual queries. The documented approximate audio-only maximum is 327 seconds; do not equate that maximum with a good retrieval window. [Model input limits](https://ai.google.dev/gemma/docs/embeddinggemma/model_card_2). FFmpeg supplies decoding and resampling. [FFmpeg resampler](https://www.ffmpeg.org/ffmpeg-resampler.html).

Do not skip silence or non-speech automatically: sound-event retrieval may matter. If silence filtering is enabled, record filtered duration and policy. For separate speakers on separate channels, a configurable per-channel representation may be preferable to downmixing; record Channel and timing. Unaligned ES transcript chunks have no invented audio timestamps. Timestamped ASR can later produce aligned transcript vectors.

Video is an optional, separately measured pass: 30-second visual windows with 5-second overlap and configurable frame sampling, plus audio segmentation. Do not promise action/region localization beyond the indexed segment.

Keep different representations separate initially: raw image/audio vectors and OCR/transcript/prose vectors can be measured and searched separately. The common vector space supports comparison, but test whether ranking all modalities together disproportionately favors one modality.

## 6. ZIP and nested-member handling

- A ZIP is a container, not content to embed directly. Each valid supported member follows the same text/image/audio pipeline.
- Members retain the owning ES DocId, source index and RootDocId; a derived MemberId/FileKey identifies a virtual file without inventing an Elasticsearch source record.
- Store archive chain and member path. File grouping uses FileKey rather than only DocId when member-level results are requested. The UI can group back to the archive-owning DocId as another presentation choice.
- Retrieve original bytes through the archive source path plus member locator; never expose a temporary extracted path as the permanent SystemPath.
- Recognize nested archives, while limiting depth, member count, expanded bytes and compression ratio. Initial configurable limits: depth 3, 10,000 members/archive, 2 GiB expanded bytes/archive, and 100 MiB per generic extracted document. Media-specific size limits differ. These are resource limits, not reasons to silently omit content.
- Check actual streamed expanded bytes, not only archive header declarations. Reject path traversal, absolute paths, links escaping the temporary root and ambiguous/unsafe member paths. Assign an entry ordinal for duplicate member names.
- Corrupt/encrypted/unsupported/oversized member: CSV event includes archive source DocId, source index and member chain; continue with siblings. Parent status becomes partial when appropriate.
- Aggregate ES Text may rescue a completely unprocessable archive. It must not be falsely attributed to a particular member, and a partial archive must not get duplicate aggregate vectors pretending that failed members were recovered.
- RAR/7z support depends on configured adapters and tools. Unsupported types still use valid ES Text fallback.

## 7. Target fields for later retrieval

Every chunk repeats the compact identity, filtering and permission fields required by retrieval. It does not repeat the full original document Text or physical bytes. A source metadata object can retain additional original non-Text fields with indexing disabled, while frequently filtered fields get explicit mappings. Inventory the real fields first; this table cannot substitute for the live mapping.

| Field group | Fields | Mapping/meaning |
| --- | --- | --- |
| Identity | ChunkId, DocId, ParentDocId, RootDocId, SourceIndex, SourceEsId, SourceKey, FileKey, ContainerFileKey, MemberId | keyword. SourceKey combines index and ES `_id`; SourceEsId preserves lookup identity even when `_id` differs from DocId. FileKey distinguishes archive members. |
| Source metadata | OriginalName, SystemPath, FileExtension, DetectedMimeType, DocumentDate, DocumentDateRaw, ReportType, Branch, SourceMetadata | Name as text plus keyword where sensible; grouping/filter fields as keyword. Explicit date mapping after inspecting source format; preserve malformed/raw value without poisoning a bulk write. SystemPath is source-only, not searchable. SourceMetadata is a nonindexed object. |
| Parent resolution | ParentSourceIndex, ParentSourceEsId, ParentResolutionStatus, MetadataOrigin | Resolve in batches; do not assume DocId equals ES `_id`. Missing-parent attachments remain independently searchable with warning. Child/parent date and Branch can differ; inherit only explicit missing values under a documented rule. |
| Modality/provenance | Modality, SourceMediaType, RepresentationKind, ContentSource, ExtractionMethod, ExtractorVersion, TextQualityStatus, TextQualityFlags, Language | keyword; distinguish original file type from embedding modality and OCR/transcript/native media. Language nullable; multilingual text need not be translated. |
| Availability | FileAvailability, FileExists, UsedTextFallback, PreviewAvailable, ProcessingStatus, CoverageComplete, WarningCodes | Boolean/keyword; missing physical files with Text remain searchable but not downloadable. FileExists can be unknown for access failures. Coverage is based on actual processing, not filename alone. |
| Text and order | ChunkText, ChunkNumber, TokenCount, CharStart, CharEnd, OffsetBasis, NormalizedTextRef, SectionPath | ChunkText=text; order/counts/offsets integer or long. CharEnd exclusive. OffsetBasis explicitly names normalized-text version. SectionPath can be null. |
| Document/table positions | PageStart, PageEnd, SlideNumber, SheetName, RowStart, RowEnd, CellRange | Optional numeric/keyword anchors only when extraction supplies them. |
| Media positions | StartSeconds, EndSeconds, MediaDurationSeconds, FrameNumber, Channel, ImageWidth, ImageHeight, BoundingBox, CoordinateSystem | Doubles/integers/explicit object. Audio seconds measured on the original timeline. BoundingBox is null unless a specific crop/region was encoded. |
| Archive locator | ArchiveChain, MemberPath, MemberOrdinal, ArchiveDepth, MemberMimeType | Explicit structured object/keywords; ArchiveChain preserves every nested container. |
| Embedding contract | Embedding, EmbeddingModel, ModelRevision, EmbeddingDimensions, EmbeddingNormalized, EmbeddingTask, ProcessorVersion, TokenizerRevision | Embedding=dense_vector, dims=768, index=true, similarity=cosine; vector index options chosen for installed ES version. No mixed dimensions/model revisions. |
| Reprocessing | SourceTextHash, ContentHash, FileSizeBytes, FileModifiedAt, SourceVersion, SourceSeqNo, SourcePrimaryTerm, ChunkingVersion, PipelineVersion, ConfigHash, GenerationId, IngestionRunId, IngestedAt | Hashes/versions keyword, sizes long, timestamps dates. ES sequence fields are scoped to their original index/shard and are not global chronological cursors. |
| Access control | TenantId, SecurityClassification, AllowedPrincipals, ACLVersion, ACLSource | Explicit keyword fields if available in the source/security system. Preserve parent/attachment permissions and apply authorization before vector retrieval and previews; Branch is not an ACL. Do not invent missing permissions. |

Do not bake query-dependent SimilarityScore, MatchType or Confidence into ingestion. Retrieval produces them later. An embedding score is not a probability, and chunks cannot know how many future query matches their file will have.

Validate IDs as strings, dates, null handling and list/scalar metadata against the mapping before indexing. Use strict explicit searchable mappings, with a nonindexed SourceMetadata container, to prevent arbitrary fields from creating a mapping explosion. Unrecognized metadata is preserved, not auto-indexed.

## 8. Streaming, concurrency and endpoint contract

Pipeline: ES reader -> durable jobs -> bounded extraction workers -> modality queues -> two embedding workers -> vector validation -> ES bulk writer -> durable completion/CSV events.

Open bounded PIT sessions and stream with search_after instead of loading millions of records. Begin with 250 source records per page and a byte limit; adapt to Text size. Persist jobs durably before advancing enumeration. Reuse the latest PIT ID and close sessions promptly. Never reuse a PIT-specific `_shard_doc` cursor with a newly opened PIT after expiration. Restart a slice and skip already completed unchanged jobs, or use a separately proven stable source-key cursor. [Elastic pagination](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results).

Use a single local SQLite WAL state store with a coordinated writer for one ingestion machine. Pending jobs, leases, progress, source generations and retries survive process death. Limit queued records and spool/cache bytes; apply backpressure. For multiple ingestion machines, replace the local queue with shared durable coordination, not SQLite on NFS.

Starting extraction concurrency: 8 lightweight text workers, 2 Office/Tika/conversion slots, 2 PDF/OCR slots, and 2 media-decoding slots, tunable to CPU/storage. Isolate risky parsers in worker processes with resource/time limits so an individual crash can be attributed to a source and retried. Decoder/render artifacts live in per-job temporary directories and are cleaned after use.

Each model endpoint initially receives one in-flight batch, independently, with separate health/latency tracking. Text batch target 16 chunks, maximum 20; image target 8, maximum 16; 60-second audio target 4, maximum 8. These are initial client budgets, not claims about H200 capacity. Increase media batches to 10-20 and add a second in-flight request per server after measuring token/byte budgets, latency and memory. Prevent slow media queues from starving text; route only to servers proven capable of that modality.

Batch by modality and approximate length; flush a partial batch after a short configurable wait. Enforce total tokens and encoded payload bytes as well as item count. An audio batch can be much larger than a text batch on the wire even when token counts look similar. Use streaming decode and short media windows; do not load an entire multi-hour recording in memory.

Before sending real records, validate synthetic text/image/audio batches against both endpoint APIs. Record supported modalities, response ordering, returned vector dimensions, normalization, payload limits, tokenizer/model revision and permitted transport (bytes/base64/URL/server-local path). Ports and model context length alone do not establish multimodal API support. A local pathname on the ingestion VM is unusable to a remote model server unless that same mount is available there.

The current vLLM development documentation includes an EmbeddingGemma2 multimodal pooling implementation for text, vision and audio; this is evidence of upstream support, not proof of the installed version's behavior. Prefer its native trained pooling implementation rather than blindly converting a generative architecture into an embedding model. Text can use `/v1/embeddings`; multimodal inputs require the structured schema/template supported by the selected vLLM version, potentially through `/v1/embeddings` messages or `/pooling`. Keep these behind a version-specific adapter and validate batching semantics: one request containing several media inputs is not automatically several independent vectors. [vLLM EmbeddingGemma2 implementation](https://docs.vllm.ai/en/latest/api/vllm/model_executor/models/embedding_gemma2/), [vLLM embedding API documentation](https://docs.vllm.ai/en/stable/models/pooling_models/embed/).

Verify embeddings have one response per input, correct ordering/dimension, finite values and nonzero norm; L2-normalize consistently. Do not index zeros or placeholder vectors. Google advises bfloat16 or float32 inference rather than float16; verify this in server configuration. [EmbeddingGemma numerical precision](https://ai.google.dev/gemma/docs/embeddinggemma/model_card_2).

Transient failure -> retry with capped exponential backoff/jitter. OOM/payload failure -> shrink batch. A bad input in a batch -> recursively split to isolate it; preserve successful inputs. A down endpoint -> health backoff and route compatible work to the other. Both down -> preserve jobs and wait/pause; do not classify every record as corrupt or drop work. Invalid global API/schema/model configuration should stop the run before corpus processing.

## 9. Failure logging and publication semantics

Write `ingestion_errors.csv` through one coordinated writer using a real CSV serializer, UTF-8 and proper quoting. Include warning/fallback/partial/skipped events as well as failures. Do not include full document text or credentials in error messages.

Columns: TimestampUtc, RunId, SourceIndex, SourceEsId, DocId, ParentDocId, RootDocId, FileKey, OriginalName, SystemPath, MemberPath, ArchiveChain, Modality, ChunkId, Stage, ErrorCode, ExceptionType, Reason, Retryable, Attempt, Endpoint, HttpStatus, Disposition, FallbackUsed.

Example dispositions: retry_scheduled, fallback_indexed, member_skipped, page_skipped, source_skipped, partial_source, failed_permanent. A recovered transient error remains traceable. Separate row-per-source status output and run summary reconcile scanned, successful, partial, fallback, skipped, failed and pending sources; these categories must not double-count the same source. Log rotation and disk space limits are required for a multi-day job.

Bulk-index in approximately 500-chunk batches, capped around 5 MiB JSON, then tune. Inspect every bulk item: a 200 HTTP response does not mean every chunk succeeded. Retry only retryable failed items. Treat mapping/data errors as item-specific, preserve context, and do not silently mark the source complete. [Elastic bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk).

Use deterministic chunk IDs derived from source identity, member identity, representation, pipeline/model version, source content generation and unit position. Resume replays overwrite the same units, not create duplicates. Do not deduplicate distinct source records out of existence just because their bytes match; reuse embeddings as a cache optimization while keeping provenance.

A source is complete only after all expected units are either indexed successfully or explicitly recorded as skipped/failed under a partial status. Store the expected unit set and counts in durable state. Never acknowledge queued or merely embedded chunks as indexed. On interruption, pending jobs/units remain retryable.

Initial v1 is a build index; publish the read alias only after the build audit/pilot acceptance. Independent chunk writes and bulk requests are not a transaction across an entire file. For a live incremental system, retrieval needs a committed-generation manifest, or whole-build versioned indexes with alias cutover; generation fields alone cannot guarantee atomic visibility. A failed replacement must not remove the prior committed generation. Do not implement an apparently atomic per-file update by deleting old chunks first.

Plan explicit source reconciliation for deletions, missing-file transitions and shorter replacements so stale chunks disappear under a controlled publication policy. No source change timestamp has been provided; DocumentDate is a business date, not an ingestion change cursor. Until a change feed/UpdatedAt field exists, schedule a fingerprint rescan plus deletion reconciliation. Enumeration recovery and incremental changes are separate concerns.

File size/mtime are a quick cache check, not proof of byte identity. SHA-256 content/text hashes verify the chosen payload, with a configurable I/O cost policy. Include parser, preprocessing, chunking and model versions in cache keys. If a missing file later appears, regenerate native media rather than permanently reusing a text-fallback status.

## 10. Validation and rollout

1. Discover actual mappings/types and audit stratified samples; confirm mounted paths and endpoint contract.
2. Prove small synthetic text/image/audio batches, document prefix behavior and finite 768d vectors on both servers.
3. Run a representative 2,000-source pilot including both indexes, missing-file Text fallback, missing-no-Text, legacy DOC/ODT, multi-page TIFF/PDF, long audio, ZIP mixtures, encrypted/corrupt members and extension mismatches.
4. Validate grouping identities and source previews/anchors. Compare hand-labeled queries by modality/language; measure Recall@k/NDCG and practical retrieval quality. Test 1,000-distinct-file/parent retrieval separately from 1,000-chunk retrieval.
5. Inject parser timeout, endpoint failure, malformed batch output, ES item rejection and process termination. Resume without duplicate/missing acknowledgements. Check corpus failure does not follow from one bad member.
6. Measure chunks/source distribution, tokens/second, audio-minutes/second, image throughput, file I/O, spool bytes, index bytes/chunk, bulk retries and memory. Size shards/storage from these measurements and installed ES vector options, not GPU model context length.
7. Scale through approximately 100,000 sources before the full corpus; keep checkpoints and do a final reconciliation of all source dispositions.

For orientation only: six million source records averaging ten embedding units would produce sixty million vectors; 768d float32 values alone represent 184.32 GB decimal. Sixty-second audio with ten-second overlap produces roughly 72 segments per hour. The actual corpus mix may change both estimates drastically; quantization, index structures, metadata and replicas alter real storage.

## 11. Implementation deliverables

- Configurable Python CLI entry point with inventory, preflight, ingest, resume, retry-failures and audit subcommands.
- Parser registry with explicit capability/dependency status and safe ZIP/member routing.
- Token-aware chunker and text/image/audio embedding adapters to the actual endpoint API.
- Version-specific Elasticsearch mapping/index/alias setup and source reader/bulk writer.
- Local durable state and leases, bounded queues/cache, graceful shutdown and reconciliation.
- Error CSV, source outcome CSV, type inventory CSV and machine-readable run summary.
- Configuration example, dependency manifest and operator instructions covering the ingestion VM's OS/mounts/tools.

Deployment addresses and serving software are known, and should be configurable rather than hardcoded in executable logic. Credentials must come from environment variables/secrets. The remaining version/API information must be resolved by production preflight for a genuine live file-type inventory and tested multimodal requests; it does not prevent offline implementation of the pipeline and these content, identity, chunking and recovery decisions.
