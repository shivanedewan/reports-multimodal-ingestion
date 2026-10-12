from __future__ import annotations

import gzip
import json
import os
import shutil
import threading
import tempfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .chunking import Chunker, Tokenizer
from .common import GlobalFailure, IngestError, canonical, digest, file_hash, normalized_text, safe_reason, source_key, usable_text, utcnow
from .elastic import Elastic
from .embeddings import Input, Scheduler
from .events import Events
from .extraction import Extractor, Notice, Unit, media_type
from .isolation import ExtractionPool
from .state import State


def iso_date(value, unit="auto"):
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)) or isinstance(value, str) and value.replace(".", "", 1).isdigit():
            number = float(value)
            divisor = 1000 if unit == "milliseconds" or unit == "auto" and abs(number) >= 100_000_000_000 else 1
            return datetime.fromtimestamp(number / divisor, timezone.utc).isoformat()
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            # Calendar/business dates carry no inferred local time-of-day.
            return parsed.date().isoformat() if "T" not in str(value) and " " not in str(value) else parsed.replace(tzinfo=timezone.utc).isoformat()
        return parsed.isoformat()
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def fingerprint(hit, config):
    if hit.get("_ingest_fingerprint"):
        return hit["_ingest_fingerprint"]
    path = Extractor(config).path(hit["_source"].get("SystemPath"))
    file_stamp = None
    try:
        if path:
            info = path.stat()
            file_stamp = [info.st_size, info.st_mtime_ns]
            if path.is_file() and config.extract.get("source_fingerprint_mode", "content") == "content":
                file_stamp.append(file_hash(path))
    except OSError as error:
        file_stamp = type(error).__name__
    value = digest([hit["_source"], hit.get("_parent"), file_stamp, config.signature])
    hit["_ingest_fingerprint"] = value
    return value


def source_metadata(hit, config):
    source = hit["_source"]
    doc = source.get("DocId")
    if doc is None or isinstance(doc, (list, dict)) or str(doc).strip() == "":
        raise IngestError("invalid_doc_id", "Missing or non-scalar DocId", stage="metadata")
    doc = str(doc)
    parent = source.get("ParentDocId")
    parent = str(parent) if parent not in (None, "", 0, "0") and not isinstance(parent, (list, dict)) else None
    attachment = hit["_index"] != config.es["source_indices"][0]
    base = {"DocId": doc, "ParentDocId": parent, "RootDocId": parent if attachment and parent else doc,
        "SourceIndex": hit["_index"], "SourceEsId": str(hit["_id"]), "SourceKey": source_key(hit),
        "OriginalName": str(source.get("OriginalName") or ""), "SystemPath": str(source.get("SystemPath") or ""),
        "FileExtension": str(source.get("FileExtension") or "").lower().strip().lstrip("."),
        "DocumentDate": iso_date(source.get("DocumentDate"), config.es.get("date_unit", "auto")),
        "DocumentDateRaw": canonical(source.get("DocumentDate")) if source.get("DocumentDate") is not None else None,
        "SourceMetadata": {k: v for k, v in source.items() if k != "Text"},
        "ParentResolutionStatus": "unresolved" if attachment and parent else "missing_reference" if attachment else "main_record",
        "SourceSeqNo": hit.get("_seq_no"), "SourcePrimaryTerm": hit.get("_primary_term"),
        "SourceVersion": str(hit.get("_version", "")), "MetadataOrigin": "source",
        "EmbeddingModel": config.embed["model"], "ModelRevision": config.embed["model_revision"],
        "EmbeddingDimensions": config.embed["dimensions"], "EmbeddingNormalized": True,
        "EmbeddingTask": "document", "TokenizerRevision": config.embed["tokenizer_revision"],
        "ProcessorVersion": config.embed["model_revision"], "ExtractorVersion": "reports_ingest-0.1.0",
        "ChunkingVersion": config.runtime["chunking_version"], "PipelineVersion": config.runtime["pipeline_version"],
        "ConfigHash": config.signature, "CoverageComplete": True, "ProcessingStatus": "indexed"}
    for name in ("ReportType", "Branch", "Language", "TenantId", "SecurityClassification", "AllowedPrincipals", "ACLVersion", "ACLSource"):
        value = source.get(name)
        if value is not None:
            base[name] = [str(v) for v in value] if isinstance(value, list) else str(value)
    # _parent is attached in memory by a bounded batch lookup, never written to source ES.
    parent_hit = hit.get("_parent")
    if parent_hit:
        base.update(ParentSourceIndex=parent_hit["_index"], ParentSourceEsId=str(parent_hit["_id"]), ParentResolutionStatus="resolved")
        inherited = []
        for name in ("Branch", "ReportType"):
            if source.get(name) in (None, "", []) and parent_hit["_source"].get(name) not in (None, "", []):
                value = parent_hit["_source"][name]
                base[name] = [str(v) for v in value] if isinstance(value, list) else str(value)
                inherited.append(name)
        if source.get("DocumentDate") in (None, ""):
            value = parent_hit["_source"].get("DocumentDate")
            date = iso_date(value, config.es.get("date_unit", "auto"))
            if date:
                base.update(DocumentDate=date, DocumentDateRaw=canonical(value))
                inherited.append("DocumentDate")
        if inherited:
            base.update(MetadataOrigin="source_with_parent_defaults", MetadataInheritedFields=inherited)
        # Permissions are intentionally NOT inferred/unioned. Retrieval must enforce the source ACL.
    return base


class TextCache:
    def __init__(self, config):
        self.config = config
        self.root = config.path(config.runtime["text_cache_path"])
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.bytes = sum(p.stat().st_size for p in self.root.rglob("*.gz"))

    def store(self, text):
        key = digest(text)
        path = self.root / key[:2] / (key + ".txt.gz")
        if path.exists():
            return key, str(path.relative_to(self.root)).replace("\\", "/")
        payload = gzip.compress(text.encode("utf-8"), mtime=0)
        with self.lock:
            if not path.exists():
                maximum = self.config.runtime["max_text_cache_bytes"]
                if maximum and self.bytes + len(payload) > maximum:
                    raise GlobalFailure("text_cache_full", "Normalized-text cache budget exhausted; increase storage budget and resume", retryable=True, stage="cache")
                if shutil.disk_usage(self.root).free < self.config.runtime["min_free_disk_bytes"] + len(payload):
                    raise GlobalFailure("disk_low", "Low cache disk space; pending jobs retained", retryable=True, stage="cache")
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary = path.with_suffix(".tmp")
                temporary.write_bytes(payload)
                os.replace(temporary, path)
                self.bytes += len(payload)
        return key, str(path.relative_to(self.root)).replace("\\", "/")


class Pipeline:
    def __init__(self, config, *, elastic=None, tokenizer=None, extractor=None, scheduler=None, run_id=None):
        self.config, self.run_id = config, run_id or uuid4().hex
        for key in ("directory", "temp_path", "text_cache_path"):
            config.path(config.runtime[key]).mkdir(parents=True, exist_ok=True)
        self.stop = threading.Event()
        self.events = Events(config.path(config.runtime["directory"]), self.run_id, config.runtime["csv_rotate_bytes"])
        self.state = State(config.path(config.runtime["state_path"]))
        self.elastic = elastic or Elastic(config)
        self.elastic.events = self.events
        self.chunker = Chunker(tokenizer or Tokenizer(config.path(config.embed["tokenizer_path"])), config.chunk)
        self.extractor = extractor or ExtractionPool(config, self.stop)
        self.scheduler = scheduler or Scheduler(config, self.stop, self.events)
        self.cache = TextCache(config)
        self.fatal = None
        self.parent_cache = {}  # Cleared each input page, bounded by page/job sizes.
        self.state.reset_running()
        self.state.meta("config_hash", config.signature)

    def check_mounts(self):
        for name in self.config.extract["required_mounts"]:
            root = Path(name)
            if not root.is_dir() or not os.access(root, os.R_OK):
                raise GlobalFailure("mount_unavailable", f"Required mount unavailable: {name}; no missing-file classifications made", retryable=True, stage="filesystem")
        for path in (self.config.path(self.config.runtime["directory"]), self.config.path(self.config.runtime["temp_path"])):
            path.mkdir(parents=True, exist_ok=True)
            if shutil.disk_usage(path).free < self.config.runtime["min_free_disk_bytes"]:
                raise GlobalFailure("disk_low", "Insufficient state/temp disk space", retryable=True, stage="filesystem")

    def documents(self, hit):
        base = source_metadata(hit, self.config)
        base["IngestionRunId"] = self.run_id
        generation = fingerprint(hit, self.config)
        base["GenerationId"] = generation
        key = base["SourceKey"]
        ordinal = 0
        if base["ParentResolutionStatus"] in {"unresolved", "missing_reference"}:
            yield Notice("parent_unresolved", "Attachment is searchable independently; parent metadata/reference could not be resolved", affects_coverage=False, stage="metadata")
        if base["DocumentDateRaw"] is not None and base["DocumentDate"] is None:
            yield Notice("invalid_document_date", "Unparseable business date retained in DocumentDateRaw", affects_coverage=False, stage="metadata")
        for unit in self.extraction_units(hit):
            if isinstance(unit, Notice):
                yield unit
                continue
            fields = {**base, **unit.fields, "Modality": unit.modality, "IngestedAt": utcnow()}
            member = fields.get("MemberId")
            file_key = digest([key, member]) if member else key
            fields["FileKey"] = file_key
            if member:
                fields["ContainerFileKey"] = key
            locator = {name: fields.get(name) for name in ("RepresentationKind", "SectionPath", "PageStart", "PageEnd", "SlideNumber", "SheetName", "RowStart", "RowEnd", "FrameNumber", "StartSeconds", "EndSeconds", "Channel")}
            if unit.modality == "text":
                text = normalized_text(unit.text)
                if not usable_text(text):
                    yield Notice("unusable_text_unit", "Text unit is empty or extraction garbage", unit.fields)
                    continue
                text_hash, reference = self.cache.store(text)
                title = fields.get("MemberPath") or fields.get("OriginalName", "")
                if fields.get("SheetName"):
                    title += " / " + fields["SheetName"]
                for chunk in self.chunker.chunks(text, title):
                    document = {**fields, "ChunkText": chunk.text, "CharStart": chunk.start, "CharEnd": chunk.end,
                        "TokenCount": chunk.tokens, "SourceTextHash": text_hash, "NormalizedTextRef": reference,
                        "OffsetBasis": "normalized_text_unicode_codepoints", "ContentHash": digest(chunk.prompt),
                        "ChunkNumber": ordinal, "ChunkId": digest([key, generation, member, locator, chunk.start, chunk.end, digest(chunk.prompt)])}
                    ordinal += 1
                    yield document, Input("text", document, prompt=chunk.prompt, tokens=chunk.tokens)
            else:
                import hashlib
                hash_ = hashlib.sha256(unit.media).hexdigest()
                tokens = math_tokens(unit)
                document = {**fields, "ContentHash": hash_, "ChunkNumber": ordinal, "TokenCount": tokens,
                    "ChunkId": digest([key, generation, member, locator, hash_])}
                ordinal += 1
                yield document, Input(unit.modality, document, media=unit.media, mime=unit.mime, tokens=tokens)

    def extraction_units(self, hit):
        emitted = False
        try:
            for value in self.extractor.record(hit):
                emitted |= isinstance(value, Unit)
                yield value
        except GlobalFailure:
            raise
        except IngestError as error:
            text = normalized_text(hit["_source"].get("Text"))
            if not emitted and usable_text(text):
                yield Notice(error.code, "Isolated parser failed; embedding ES Text fallback", disposition="fallback_indexed", affects_coverage=False)
                yield Unit("text", {"ContentSource": "es_text", "UsedTextFallback": True, "RepresentationKind": "es_text_fallback",
                    "SourceMediaType": media_type(str(hit["_source"].get("FileExtension", "")).lower().lstrip(".")),
                    "FileAvailability": "unknown", "PreviewAvailable": False, "ExtractionMethod": "es_text", "TextQualityStatus": "usable"}, text=text)
            else:
                yield Notice(error.code, str(error), disposition="source_partial" if emitted else "source_skipped")

    def process(self, hit):
        key = source_key(hit)
        if not self.state.claim(key):
            return
        source = hit["_source"]
        context = {"SourceIndex": hit["_index"], "SourceEsId": str(hit["_id"]), "DocId": source.get("DocId"),
            "ParentDocId": source.get("ParentDocId"), "OriginalName": source.get("OriginalName"), "SystemPath": source.get("SystemPath")}
        pending, bulk, bulk_bytes = deque(), [], 0
        spool = tempfile.TemporaryFile(mode="w+t", encoding="utf-8", dir=self.config.path(self.config.runtime["temp_path"]))
        indexed = failed = warnings = 0
        coverage, fallback = True, False
        warning_codes = set()
        rejected_ids = set()
        generation = fingerprint(hit, self.config)

        def flush():
            nonlocal indexed, failed, bulk_bytes
            if not bulk:
                return
            rejected = self.elastic.bulk(bulk)
            indexed += len(bulk) - len(rejected)
            failed += len(rejected)
            for document, error in rejected:
                rejected_ids.add(document["ChunkId"])
                self.events.error(document, error, "chunk_failed")
            bulk.clear(); bulk_bytes = 0

        def resolve():
            nonlocal failed, bulk_bytes, fallback
            document, future = pending.popleft()
            try:
                document["Embedding"] = future.result()
            except GlobalFailure:
                raise
            except Exception as error:
                failed += 1
                self.events.error(document, error, "chunk_failed")
                return
            size = len(canonical(document).encode("utf-8")) + 256
            if size > self.config.es["bulk_bytes"]:
                failed += 1
                self.events.error(document, IngestError("bulk_unit_size_limit", "Single chunk metadata/content exceeds bulk budget", stage="index"), "chunk_failed")
                return
            fallback |= bool(document.get("UsedTextFallback"))
            # Disk spool allows final source coverage to be known before chunk writes, without
            # keeping every vector of a large document/recording in RAM.
            if shutil.disk_usage(self.config.path(self.config.runtime["temp_path"])).free < self.config.runtime["min_free_disk_bytes"] + size:
                raise GlobalFailure("disk_low", "Insufficient vector spool disk; source retained", retryable=True, stage="spool")
            spool.write(canonical(document) + "\n")

        try:
            self.check_mounts()
            iterator = self.documents(hit)
            try:
                for item in iterator:
                    if self.stop.is_set():
                        raise self.fatal or GlobalFailure("run_stopped", "Source retained for resume", retryable=True)
                    if isinstance(item, Notice):
                        warnings += 1
                        warning_codes.add(item.code)
                        coverage &= not item.affects_coverage
                        self.events.error({**context, **item.fields}, IngestError(item.code, item.reason, stage=item.stage), item.disposition,
                            FallbackUsed=item.disposition == "fallback_indexed")
                        continue
                    document, value = item
                    pending.append((document, self.scheduler.submit(value)))
                    if len(pending) >= self.config.extract["max_pending_chunks_per_source"]:
                        resolve()
            except IngestError as error:
                if isinstance(error, GlobalFailure):
                    raise
                warnings += 1
                warning_codes.add(error.code)
                coverage = False
                self.events.error(context, error, "source_partial" if indexed or pending else "source_skipped")
            finally:
                iterator.close()
            while pending:
                resolve()
            spool.seek(0)
            for line in spool:
                document = json.loads(line)
                document["CoverageComplete"] = coverage and not failed
                document["WarningCodes"] = sorted(warning_codes)
                document["ProcessingStatus"] = "partial" if not coverage or failed else "fallback" if fallback else "complete"
                size = len(line.encode("utf-8")) + 256
                if bulk and (len(bulk) >= self.config.es["bulk_items"] or bulk_bytes + size > self.config.es["bulk_bytes"]):
                    flush()
                bulk.append(document); bulk_bytes += size
            flush()
            if rejected_ids:
                # Rare mapping/data failures: repair coverage metadata on successful sibling chunks.
                # Re-read bounded spool rather than retaining their IDs/vectors in memory.
                spool.seek(0)
                for line in spool:
                    document = json.loads(line)
                    if document["ChunkId"] in rejected_ids:
                        continue
                    document.update(CoverageComplete=False, ProcessingStatus="partial", WarningCodes=sorted(warning_codes | {"bulk_item_failed"}))
                    size = len(line.encode("utf-8")) + 256
                    if bulk and (len(bulk) >= self.config.es["bulk_items"] or bulk_bytes + size > self.config.es["bulk_bytes"]):
                        # These are metadata repairs, not additional indexed chunk counts.
                        repairs = self.elastic.bulk(bulk)
                        if repairs:
                            raise GlobalFailure("coverage_repair_failed", "Sibling coverage metadata repair failed; source retained", stage="index")
                        bulk.clear(); bulk_bytes = 0
                    bulk.append(document); bulk_bytes += size
                if bulk:
                    if self.elastic.bulk(bulk):
                        raise GlobalFailure("coverage_repair_failed", "Sibling coverage metadata repair failed; source retained", stage="index")
                    bulk.clear()
            status = "failed" if failed and not indexed else "partial" if failed or not coverage and indexed else "fallback" if fallback else "complete" if indexed else "skipped"
            self.state.finish(key, status, indexed, failed, warnings, generation)
            self.events.outcome(**context, Status=status, ChunksIndexed=indexed, UnitsFailed=failed, WarningCount=warnings, GenerationId=generation)
        except GlobalFailure as error:
            self.fatal = error
            self.stop.set()
            self.events.error(context, error, "source_pending")
            self.state.finish(key, "pending", indexed, failed, warnings, generation, safe_reason(error))
        except Exception as error:
            self.events.error(context, error, "source_failed")
            self.state.finish(key, "failed", indexed, failed + 1, warnings, generation, type(error).__name__)
        finally:
            # Futures already sent can finish; these IDs will be replayed on resume if unacknowledged.
            pending.clear()
            spool.close()

    def run(self, *, limit=None, retry_failures=False, resume=True):
        outstanding = set()
        source_bytes = {}
        scheduled = 0
        prior = self.state.meta("enumeration_complete")
        self.state.meta("enumeration_complete", False)
        complete_scan = False
        max_jobs = self.config.extract["max_pending_jobs"]
        try:
            self.check_mounts()
            with ThreadPoolExecutor(max_workers=self.config.extract["workers"]) as executor:
                def submit(hit, force=False):
                    nonlocal scheduled, outstanding
                    hit.pop("_ingest_fingerprint", None)  # Revalidate retained jobs in a fresh run.
                    estimated = len(canonical(hit).encode("utf-8"))
                    maximum_bytes = self.config.extract.get("max_pending_source_bytes", 268435456)
                    while (len(outstanding) >= max_jobs or outstanding and sum(source_bytes.values()) + estimated > maximum_bytes) and not self.stop.is_set():
                        done, outstanding = wait(outstanding, timeout=0.25, return_when=FIRST_COMPLETED)
                        for future in done:
                            source_bytes.pop(future, None)
                            future.result()
                    if self.stop.is_set():
                        return False
                    if self.state.enqueue(hit, fingerprint(hit, self.config), self.run_id, force):
                        future = executor.submit(self.process, hit)
                        outstanding.add(future)
                        source_bytes[future] = estimated
                        scheduled += 1
                        if scheduled % 100 == 0:
                            print(f"Scheduled {scheduled} sources; {self.state.summary()}", flush=True)
                    return True

                if resume or retry_failures:
                    statuses = ("pending", "partial", "failed") if retry_failures else ("pending",)
                    for hit in self.state.retained(statuses):
                        if not submit(hit, force=retry_failures):
                            break
                        if limit and scheduled >= limit:
                            break
                if not retry_failures and not self.stop.is_set() and not (limit and scheduled >= limit):
                    for index in self.config.es["source_indices"]:
                        page = []
                        page_bytes = 0
                        for hit in self.elastic.scan(index, stop=self.stop):
                            page.append(hit)
                            page_bytes += len(canonical(hit).encode("utf-8"))
                            if len(page) >= self.config.es["page_size"] or page_bytes >= self.config.runtime.get("max_http_response_bytes", 67108864):
                                self.enrich(page)
                                for source in page:
                                    submit(source)
                                    if limit and scheduled >= limit:
                                        break
                                page.clear()
                                page_bytes = 0
                                if self.stop.is_set() or limit and scheduled >= limit:
                                    break
                        if page and not self.stop.is_set():
                            self.enrich(page)
                            for source in page:
                                if limit and scheduled >= limit:
                                    break
                                submit(source)
                        if self.stop.is_set() or limit and scheduled >= limit:
                            break
                    else:
                        complete_scan = True
                for future in outstanding:
                    future.result()
                if not self.stop.is_set():
                    if retry_failures:
                        self.state.meta("enumeration_complete", bool(prior))
                    elif complete_scan:
                        self.state.meta("enumeration_complete", True)
                        self.state.meta("full_scan_run", self.run_id)
        except BaseException:
            self.stop.set()
            raise
        finally:
            self.close()
        if self.fatal:
            raise self.fatal
        return self.summary

    def enrich(self, hits):
        ids = {str(hit["_source"].get("ParentDocId")) for hit in hits if hit["_index"] != self.config.es["source_indices"][0] and hit["_source"].get("ParentDocId") not in (None, "", 0, "0")}
        if not ids:
            return
        found = self.elastic.parents(ids)
        for hit in hits:
            parent = found.get(str(hit["_source"].get("ParentDocId")))
            if parent:
                hit["_parent"] = parent

    def close(self):
        self.scheduler.close()
        if hasattr(self.extractor, "close"):
            self.extractor.close()
        self.summary = self.state.summary()
        path = self.config.path(self.config.runtime["directory"]) / f"run-{self.run_id}.json"
        path.write_text(json.dumps({"run_id": self.run_id, "config_hash": self.config.signature,
            "summary": self.summary, "enumeration_complete": self.state.meta("enumeration_complete"),
            "fatal": self.fatal.code if self.fatal else None}, indent=2), encoding="utf-8")
        self.state.close()
        self.elastic.close()


def math_tokens(unit):
    import math
    if unit.modality == "audio":
        return math.ceil((unit.fields["EndSeconds"] - unit.fields["StartSeconds"]) * 25)
    return 1120  # Upper bound used for client batch budgeting, not exact processor token count.

