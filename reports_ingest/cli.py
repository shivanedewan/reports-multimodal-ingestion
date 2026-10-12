from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from collections import Counter
from pathlib import Path
from uuid import uuid4

from .chunking import Tokenizer
from .common import GlobalFailure, IngestError, extension, source_key, utcnow
from .config import Config
from .elastic import Elastic
from .embeddings import EmbeddingClient
from .events import Events
from .extraction import Extractor, Notice, Unit, registry
from .pipeline import Pipeline
from .schema import index_schema
from .state import State


def save(config, name, value):
    path = config.path(config.runtime["directory"]) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(path))


def deployment_preflight(config, elastic, *, local_only=False):
    config.require_pinned_model()
    tokenizer = Tokenizer(config.path(config.embed["tokenizer_path"]))
    if not tokenizer.count("title: none | text: Preflight."):
        raise GlobalFailure("invalid_tokenizer", "Tokenizer produced no input IDs", stage="preflight")
    tools = registry(config)
    if "audio" in config.embed["modalities"] and (not tools["dependencies"]["ffmpeg"] or not tools["dependencies"]["ffprobe"]):
        raise GlobalFailure("audio_tools_missing", "Audio enabled but ffmpeg/ffprobe are unavailable", stage="preflight")
    for mount in config.extract["required_mounts"]:
        if not Path(mount).is_dir() or not os.access(mount, os.R_OK):
            raise GlobalFailure("mount_unavailable", f"Configured source mount unavailable: {mount}", stage="preflight")
    for key in ("directory", "temp_path", "text_cache_path"):
        path = config.path(config.runtime[key])
        path.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(path).free < config.runtime["min_free_disk_bytes"]:
            raise GlobalFailure("disk_low", "Insufficient free deployment disk", stage="preflight")
    report = {"at": utcnow(), "config_hash": config.signature, "capabilities": tools,
        "tokenizer_tokens": tokenizer.count("title: none | text: Preflight."), "remote_checked": not local_only}
    if not local_only:
        report["elasticsearch"] = elastic.inspect()
        report["embedding"] = []
        for endpoint in config.embed["endpoints"]:
            client = EmbeddingClient(config, endpoint)
            try:
                report["embedding"].append(client.preflight())
            finally:
                client.close()
    save(config, "preflight.json", report)
    return report


def inventory(config, elastic, limit=None):
    # Streaming fallback works even when FileExtension is text/non-aggregatable or absent.
    import sqlite3
    import tempfile
    runtime = config.path(config.runtime["directory"])
    runtime.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix="inventory-", suffix=".sqlite", dir=runtime)
    os.close(handle)
    database = sqlite3.connect(temporary)
    database.execute("CREATE TABLE counts(source TEXT,raw TEXT,ext TEXT,n INTEGER,text_n INTEGER,sample TEXT,PRIMARY KEY(source,raw,ext))")
    from .common import usable_text
    scanned = 0
    try:
        for name in config.es["source_indices"]:
            for hit in elastic.scan(name):
                source = hit["_source"]
                raw = str(source.get("FileExtension") or "")
                filename = Path(str(source.get("OriginalName") or source.get("SystemPath") or "")).name
                inferred = raw or (extension(filename) if "." in filename else "[missing]")
                key = (hit["_index"], raw, extension(inferred) if inferred else "[missing]")
                database.execute("INSERT INTO counts VALUES(?,?,?,1,?,?) ON CONFLICT(source,raw,ext) DO UPDATE SET n=n+1,text_n=text_n+excluded.text_n",
                    (*key, int(usable_text(source.get("Text"))), str(source.get("DocId") or hit["_id"])))
                scanned += 1
                if scanned % 1000 == 0:
                    database.commit()
                if limit and scanned >= limit:
                    break
            if limit and scanned >= limit:
                break
        path = runtime / "file_type_inventory.csv"
        with path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["SourceIndex", "RawExtension", "NormalizedExtension", "Records", "UsableTextRecords", "ExampleDocId", "Scope"])
            for row in database.execute("SELECT * FROM counts ORDER BY source,raw,ext"):
                writer.writerow([*row, "sample" if limit else "full_stream"])
        print(str(path))
    finally:
        database.close()
        Path(temporary).unlink(missing_ok=True)


def audit(config, elastic, samples_per_type=3):
    counts = Counter()
    events = Events(config.path(config.runtime["directory"]), "audit-" + uuid4().hex, config.runtime["csv_rotate_bytes"])
    # Use the same crash-isolated pipeline extractor, no model/index writes.
    import threading
    from .isolation import ExtractionPool
    pool = ExtractionPool(config, threading.Event())
    path = config.path(config.runtime["directory"]) / "file_access_audit.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["SourceIndex", "DocId", "Extension", "Units", "Warnings", "FallbackUnits", "FileAvailability", "Scope"])
            for name in config.es["source_indices"]:
                for hit in elastic.scan(name):
                    source = hit["_source"]
                    ext = extension(str(source.get("FileExtension") or source.get("OriginalName") or ""))
                    key = (hit["_index"], ext, bool(source.get("Text")), str(source.get("Branch", "")))
                    if counts[key] >= samples_per_type:
                        continue
                    counts[key] += 1
                    units = warnings = fallbacks = 0
                    availability = "unknown"
                    try:
                        for item in pool.record(hit):
                            if isinstance(item, Notice):
                                warnings += 1
                                events.error({"SourceIndex": hit["_index"], "DocId": source.get("DocId"), **item.fields},
                                    IngestError(item.code, item.reason), item.disposition)
                            else:
                                units += 1
                                fallbacks += bool(item.fields.get("UsedTextFallback"))
                            availability = item.fields.get("FileAvailability", availability)
                    except IngestError as error:
                        warnings += 1
                        events.error({"SourceIndex": hit["_index"], "DocId": source.get("DocId")}, error, "audit_failed")
                    writer.writerow([hit["_index"], source.get("DocId"), ext, units, warnings, fallbacks, availability, "stratified_sample"])
    finally:
        pool.close()
    print(str(path))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Resumable reports multimodal ingestion (no credentials in config)")
    parser.add_argument("--config", default="config.local.json")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("capabilities")
    sub.add_parser("mapping")
    pre = sub.add_parser("preflight"); pre.add_argument("--local-only", action="store_true")
    inv = sub.add_parser("inventory"); inv.add_argument("--limit", type=int)
    aud = sub.add_parser("audit"); aud.add_argument("--samples-per-type", type=int, default=3)
    for name in ("ingest", "resume", "retry-failures"):
        run = sub.add_parser(name)
        run.add_argument("--limit", type=int)
        run.add_argument("--create-index", action="store_true")
    sub.add_parser("status")
    pub = sub.add_parser("publish"); pub.add_argument("--allow-partial", action="store_true")
    inspect = sub.add_parser("inspect-file")
    inspect.add_argument("path"); inspect.add_argument("--doc-id", default="offline-test")
    args = parser.parse_args(argv)
    elastic = None
    run_lock = None
    try:
        config = Config.load(args.config)
        if args.command == "capabilities":
            print(json.dumps(registry(config), indent=2)); return 0
        if args.command == "mapping":
            print(json.dumps(index_schema(config), indent=2)); return 0
        if args.command == "status":
            state = State(config.path(config.runtime["state_path"]))
            try:
                print(json.dumps({"summary": state.summary(), "enumeration_complete": state.meta("enumeration_complete")}, indent=2))
            finally:
                state.close()
            return 0
        if args.command == "inspect-file":
            config.path(config.runtime["temp_path"]).mkdir(parents=True, exist_ok=True)
            path = Path(args.path).resolve()
            hit = {"_index": config.es["source_indices"][0], "_id": args.doc_id,
                "_source": {"DocId": args.doc_id, "OriginalName": path.name, "SystemPath": str(path)}}
            import threading
            from .isolation import ExtractionPool
            pool = ExtractionPool(config, threading.Event())
            try:
                for item in pool.record(hit):
                    if isinstance(item, Notice):
                        print(json.dumps({"notice": item.code, "reason": item.reason, "fields": item.fields}, ensure_ascii=False))
                    else:
                        print(json.dumps({"modality": item.modality, "characters": len(item.text or ""), "media_bytes": len(item.media or b""), "fields": item.fields}, ensure_ascii=False))
            finally:
                pool.close()
            return 0
        elastic = Elastic(config)
        if args.command in {"ingest", "resume", "retry-failures", "publish"}:
            from .locking import RunLock
            run_lock = RunLock(config.path(config.runtime["state_path"]))
            run_lock.__enter__()
        if args.command == "preflight":
            deployment_preflight(config, elastic, local_only=args.local_only)
        elif args.command == "inventory":
            elastic.inspect(); inventory(config, elastic, args.limit)
        elif args.command == "audit":
            elastic.inspect(); audit(config, elastic, args.samples_per_type)
        elif args.command == "publish":
            state = State(config.path(config.runtime["state_path"]))
            try:
                build_id = state.meta("build_id")
                if not build_id:
                    raise GlobalFailure("build_state_missing", "Publication requires the original ingestion state database", stage="publish")
                elastic.ensure_index(build_id=build_id)
                summary = state.summary()
                if state.meta("config_hash") != config.signature or not state.meta("enumeration_complete"):
                    raise GlobalFailure("build_incomplete", "Publish requires a full enumeration for this exact build", stage="publish")
                if summary.get("pending") or summary.get("running"):
                    raise GlobalFailure("build_pending", "Pending/running sources prevent publication", stage="publish")
                if not args.allow_partial and (summary.get("failed") or summary.get("partial")):
                    raise GlobalFailure("build_partial", "Review source outcomes; retry failures or explicitly publish --allow-partial", stage="publish")
                run = state.meta("full_scan_run")
                if state.unseen(run):
                    raise GlobalFailure("source_reconciliation_required", "Sources disappeared since an earlier scan; build a new target/state before publication", stage="publish")
                expected = sum(v["chunks"] or 0 for v in summary.values())
                elastic.http.json("POST", f"/{config.es['target_index']}/_refresh")
                actual = elastic.http.json("GET", f"/{config.es['target_index']}/_count")["count"]
                if actual != expected:
                    raise GlobalFailure("chunk_count_mismatch", f"Target contains {actual} chunks; state confirms {expected}; reconcile before publication", stage="publish")
                elastic.publish()
                print(f"Published {config.es['target_index']} through {config.es['read_alias']}")
            finally:
                state.close()
        else:
            deployment_preflight(config, elastic)
            state = State(config.path(config.runtime["state_path"]))
            try:
                previous = state.meta("config_hash")
                if previous and previous != config.signature:
                    raise GlobalFailure("state_contract_mismatch", "State belongs to a different build; choose a new state_path", stage="preflight")
                build_id = state.meta("build_id") or state.meta("build_id", uuid4().hex)
                elastic.ensure_index(args.create_index, build_id=build_id)
                if elastic.published():
                    raise GlobalFailure("published_build_immutable", "Published indexes are immutable here; use a new versioned target/state", stage="preflight")
            finally:
                state.close()
            pipeline = Pipeline(config, elastic=elastic)
            elastic = None  # Pipeline owns and closes it.
            print(json.dumps(pipeline.run(limit=args.limit, retry_failures=args.command == "retry-failures", resume=True), indent=2))
        return 0
    except KeyboardInterrupt:
        print("Interrupted. Durable pending work can be resumed.", file=sys.stderr)
        return 130
    except (IngestError, ValueError, OSError, KeyError) as error:
        from .common import safe_reason
        print(f"{getattr(error, 'code', type(error).__name__)}: {safe_reason(error)}", file=sys.stderr)
        return 2
    finally:
        if elastic:
            elastic.close()
        if run_lock and run_lock.stream and not run_lock.stream.closed:
            run_lock.__exit__()

