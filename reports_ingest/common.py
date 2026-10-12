from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class IngestError(Exception):
    def __init__(self, code: str, reason: str, *, retryable: bool = False, stage: str = "extract"):
        super().__init__(reason)
        self.code, self.retryable, self.stage = code, retryable, stage


class GlobalFailure(IngestError):
    """Infrastructure/contract failure: stop accepting work, retain pending jobs."""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def file_hash(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def normalized_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.replace("\r\n", "\n").replace("\r", "\n").strip()


def usable_text(value: Any) -> bool:
    text = normalized_text(value)
    if not text or not any(c.isalnum() for c in text):
        return False
    bad = sum(c == "\x00" or (ord(c) < 32 and c not in "\n\t") or c == "\ufffd" for c in text)
    return bad / len(text) < 0.05 and not re.fullmatch(
        r"(?is)(?:error extracting text|text extraction failed|unsupported file format|no text extracted)[.!\s]*", text
    )


def safe_reason(value: Any) -> str:
    # Never log request/response bodies; scrub common credential-bearing exception strings.
    text = str(value).replace("\x00", "")[:2000]
    text = re.sub(r"(?i)(https?://)[^/\s@]+@", r"\1[redacted]@", text)
    return re.sub(r"(?i)(password|authorization|api[_-]?key|bearer)(\s*[:=]?\s*)[^\s,;]+", r"\1\2[redacted]", text)


def source_key(hit: dict) -> str:
    return digest([hit["_index"], str(hit["_id"])])


def extension(name: str) -> str:
    value = name.lower().strip().lstrip(".")
    for ending in ("tar.gz", "tar.bz2", "tar.xz"):
        if value.endswith(ending):
            return ending
    return value.rsplit(".", 1)[-1] if "." in value else value

