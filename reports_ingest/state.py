from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from .common import canonical, source_key, utcnow


class State:
    """Single-process coordinated SQLite WAL; completed jobs discard large source payloads."""
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS jobs (
              key TEXT PRIMARY KEY, source_index TEXT, source_id TEXT, doc_id TEXT,
              fingerprint TEXT, payload TEXT, status TEXT, chunks INTEGER DEFAULT 0,
              failures INTEGER DEFAULT 0, warnings INTEGER DEFAULT 0, generation TEXT,
              reason TEXT, updated TEXT, seen_run TEXT);
            CREATE INDEX IF NOT EXISTS job_status ON jobs(status);
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        """)
        self.db.commit()

    @contextmanager
    def transaction(self):
        with self.lock:
            with self.db:
                yield self.db

    def enqueue(self, hit: dict, fingerprint: str, run_id: str, force: bool = False) -> bool:
        key = source_key(hit)
        with self.transaction() as db:
            old = db.execute("SELECT fingerprint,status FROM jobs WHERE key=?", (key,)).fetchone()
            if old and old["fingerprint"] != fingerprint:
                # Live replacement cannot be atomic across chunks. Require a new build index/state.
                raise ValueError(f"Changed source {hit['_index']}/{hit['_id']}: build a new versioned target/state")
            if old and old["fingerprint"] == fingerprint and old["status"] in {"complete", "fallback", "skipped", "partial", "failed"} and not force:
                db.execute("UPDATE jobs SET seen_run=? WHERE key=?", (run_id, key))
                return False
            if old and old["status"] == "running":
                db.execute("UPDATE jobs SET seen_run=? WHERE key=?", (run_id, key))
                return False
            db.execute("""INSERT INTO jobs(key,source_index,source_id,doc_id,fingerprint,payload,status,updated,seen_run)
                VALUES(?,?,?,?,?,?,'pending',?,?) ON CONFLICT(key) DO UPDATE SET
                fingerprint=excluded.fingerprint,payload=excluded.payload,status='pending',
                chunks=0,failures=0,warnings=0,reason=NULL,updated=excluded.updated,seen_run=excluded.seen_run""",
                (key, hit["_index"], str(hit["_id"]), str(hit["_source"].get("DocId", "")), fingerprint, canonical(hit), utcnow(), run_id))
            return True

    def claim(self, key: str) -> bool:
        with self.transaction() as db:
            return db.execute("UPDATE jobs SET status='running',updated=? WHERE key=? AND status='pending'", (utcnow(), key)).rowcount == 1

    def finish(self, key: str, status: str, chunks=0, failures=0, warnings=0, generation=None, reason=""):
        with self.transaction() as db:
            db.execute("""UPDATE jobs SET status=?,chunks=?,failures=?,warnings=?,generation=?,reason=?,updated=?,
                payload=CASE WHEN ? IN ('complete','fallback','skipped') THEN NULL ELSE payload END WHERE key=?""",
                (status, chunks, failures, warnings, generation, reason, utcnow(), status, key))

    def reset_running(self):
        with self.transaction() as db:
            db.execute("UPDATE jobs SET status='pending' WHERE status='running'")

    def retained(self, statuses=("pending",)):
        # Keyset pages release DB lock before yielding; no millions-of-jobs list in memory.
        last = ""
        while True:
            with self.lock:
                marks = ",".join("?" for _ in statuses)
                rows = self.db.execute(f"SELECT key,payload FROM jobs WHERE key>? AND status IN ({marks}) AND payload IS NOT NULL ORDER BY key LIMIT 128", (last, *statuses)).fetchall()
            if not rows:
                return
            for row in rows:
                last = row["key"]
                yield json.loads(row["payload"])

    def meta(self, key: str, value=None):
        with self.transaction() as db:
            if value is not None:
                db.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, canonical(value)))
                return value
            row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else None

    def summary(self):
        with self.lock:
            return {r["status"]: {"sources": r["n"], "chunks": r["chunks"], "failures": r["failures"]} for r in self.db.execute(
                "SELECT status,count(*) n,sum(chunks) chunks,sum(failures) failures FROM jobs GROUP BY status")}

    def unseen(self, run_id):
        with self.lock:
            return self.db.execute("SELECT count(*) FROM jobs WHERE seen_run!=? OR seen_run IS NULL", (run_id,)).fetchone()[0]

    def close(self):
        self.db.close()

