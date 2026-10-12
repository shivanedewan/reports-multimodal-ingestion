from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path

from .common import GlobalFailure, digest, file_hash


class Config:
    def __init__(self, data: dict, base: Path = Path(".")):
        self.data = deepcopy(data)
        self.base = base.resolve()
        self.es = self.data["elasticsearch"]
        self.embed = self.data["embedding"]
        self.chunk = self.data["chunking"]
        self.extract = self.data["extraction"]
        self.runtime = self.data["runtime"]
        self._signature = None
        self.validate()

    @classmethod
    def load(cls, path: str):
        file = Path(path).resolve()
        return cls(json.loads(file.read_text(encoding="utf-8")), file.parent)

    def path(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else self.base / path

    def secret(self, section: dict, key: str) -> str:
        return os.environ.get(section.get(key, ""), "")

    @property
    def signature(self) -> str:
        if self._signature is not None:
            return self._signature
        # Exclude operational tuning/addresses so a worker-count change does not re-embed everything.
        tokenizer = self.path(self.embed["tokenizer_path"])
        self._signature = digest({
            "model": self.embed["model"], "revision": self.embed["model_revision"],
            "tokenizer": self.embed["tokenizer_revision"], "dims": self.embed["dimensions"],
            "chunking": self.chunk, "extract": {k: v for k, v in self.extract.items()
                if k not in {"workers", "max_pending_jobs", "max_pending_chunks_per_source", "required_mounts"}},
            "media_transport": self.embed["media_transport"], "media_extra_body": self.embed.get("media_extra_body", {}),
            "pipeline": self.runtime["pipeline_version"], "chunking_version": self.runtime["chunking_version"],
            "target": self.es["target_index"], "date_unit": self.es.get("date_unit", "auto"),
            "sources": self.es["source_indices"], "query": self.es["query"], "parent_lookup_field": self.es["parent_lookup_field"],
            "tokenizer_hash": file_hash(tokenizer) if tokenizer.is_file() else None,
        })
        return self._signature

    def validate(self):
        c, e, x = self.chunk, self.embed, self.extract
        if not 0 <= c["overlap_tokens"] < c["target_tokens"] < c["max_input_tokens"] <= 8192:
            raise ValueError("Require 0 <= overlap < target < max_input <= 8192")
        if not 0 <= x["audio_overlap_seconds"] < x["audio_window_seconds"] <= 300:
            raise ValueError("Invalid audio window/overlap")
        if x["audio_sample_rate"] != 16000:
            raise ValueError("EmbeddingGemma 2 audio requires 16 kHz")
        if x.get("source_fingerprint_mode", "content") not in {"content", "stat"}:
            raise ValueError("source_fingerprint_mode must be content or stat")
        if e["dimensions"] != 768:
            raise ValueError("This v1 build uses native 768d vectors")
        if not e["endpoints"] or not e["modalities"] or set(e["modalities"]) - {"text", "image", "audio"}:
            raise ValueError("Configure endpoints and text/image/audio modalities")
        if e["media_transport"] not in {"messages", "cohere_images"}:
            raise ValueError("media_transport must be messages or cohere_images")
        if self.es["target_index"] in self.es["source_indices"]:
            raise ValueError("Target must differ from source indexes")
        if any(k in e.get("media_extra_body", {}) for k in ("model", "input", "messages", "encoding_format")):
            raise ValueError("media_extra_body cannot override model/input/messages/encoding_format")
        positive = [(e, k) for k in ("text_batch", "image_batch", "audio_batch", "queue_items", "batch_tokens", "batch_bytes", "media_concurrency")]
        positive += [(x, k) for k in ("workers", "max_pending_jobs", "max_pending_chunks_per_source", "archive_members", "archive_expanded_bytes", "max_document_bytes")]
        positive += [(self.es, k) for k in ("page_size", "bulk_items", "bulk_bytes", "shards")]
        if any(section[key] <= 0 for section, key in positive):
            raise ValueError("Batch, queue, worker and byte limits must be positive")

    def require_pinned_model(self):
        for key in ("model_revision", "tokenizer_revision"):
            value = self.embed[key]
            if not value or value in {"main", "latest"} or value.startswith("REPLACE"):
                raise GlobalFailure("model_revision_required", f"Set embedding.{key} to the deployed immutable revision", stage="preflight")

