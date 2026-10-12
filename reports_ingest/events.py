from __future__ import annotations

import csv
import json
import threading
from pathlib import Path

from .common import safe_reason, utcnow

ERROR_COLUMNS = "TimestampUtc RunId SourceIndex SourceEsId DocId ParentDocId RootDocId FileKey OriginalName SystemPath MemberPath ArchiveChain Modality ChunkId Stage ErrorCode ExceptionType Reason Retryable Attempt Endpoint HttpStatus Disposition FallbackUsed".split()
OUTCOME_COLUMNS = "TimestampUtc RunId SourceIndex SourceEsId DocId Status ChunksIndexed UnitsFailed WarningCount GenerationId Reason".split()


class Events:
    def __init__(self, directory: Path, run_id: str, rotate_bytes: int = 100 * 1024 * 1024):
        directory.mkdir(parents=True, exist_ok=True)
        self.directory, self.run_id, self.rotate_bytes = directory, run_id, rotate_bytes
        self.lock = threading.Lock()

    def _write(self, name: str, columns: list, values: dict):
        with self.lock:
            path = self.directory / name
            if path.exists() and path.stat().st_size >= self.rotate_bytes:
                suffix = 1
                while path.with_name(f"{path.stem}.{suffix}.csv").exists():
                    suffix += 1
                path.rename(path.with_name(f"{path.stem}.{suffix}.csv"))
            new = not path.exists() or path.stat().st_size == 0
            with path.open("a", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
                if new:
                    writer.writeheader()
                writer.writerow({"TimestampUtc": utcnow(), "RunId": self.run_id, **values})

    def error(self, context: dict, error, disposition: str, **extra):
        values = {**context, "ErrorCode": getattr(error, "code", "unexpected_error"),
            "ExceptionType": type(error).__name__, "Reason": safe_reason(error),
            "Stage": getattr(error, "stage", "extract"), "Retryable": getattr(error, "retryable", False),
            "Disposition": disposition, **extra}
        if isinstance(values.get("ArchiveChain"), (list, dict)):
            values["ArchiveChain"] = json.dumps(values["ArchiveChain"], ensure_ascii=False)
        self._write("ingestion_errors.csv", ERROR_COLUMNS, values)

    def outcome(self, **values):
        self._write("source_outcomes.csv", OUTCOME_COLUMNS, values)

