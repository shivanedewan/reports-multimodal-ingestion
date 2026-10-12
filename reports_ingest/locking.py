from __future__ import annotations

import os
from pathlib import Path

from .common import GlobalFailure


class RunLock:
    """OS releases the single-ingestion-process lock even after a crash."""
    def __init__(self, state_path: Path):
        self.path = state_path.with_suffix(state_path.suffix + ".lock")
        self.stream = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+b")
        self.stream.seek(0)
        if os.fstat(self.stream.fileno()).st_size == 0:
            self.stream.write(b"0"); self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            raise GlobalFailure("ingestion_already_running", "Another ingestion/publication process owns this state database", stage="state") from None
        return self

    def __exit__(self, *args):
        if self.stream:
            self.stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream, fcntl.LOCK_UN)
            self.stream.close()

