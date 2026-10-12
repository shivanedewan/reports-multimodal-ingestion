import json
from concurrent.futures import Future

import pytest

from reports_ingest.common import GlobalFailure, IngestError, source_key
from reports_ingest.extraction import Extractor
from reports_ingest.pipeline import Pipeline
from reports_ingest.state import State
from .conftest import hit


class ImmediateScheduler:
    def submit(self, value):
        future = Future(); future.set_result([1.] + [0.] * 767); return future
    def close(self):
        pass


class MemoryElastic:
    def __init__(self, sources, crash=False, reject_ordinal=None):
        self.sources = sources; self.documents = {}; self.crash = crash; self.reject = reject_ordinal
    def scan(self, index, **kwargs):
        yield from (s for s in self.sources if s["_index"] == index)
    def parents(self, ids):
        return {}
    def bulk(self, documents):
        failures = []
        for document in documents:
            if document["ChunkNumber"] == self.reject:
                failures.append((document, IngestError("mapping_rejected", "test rejection", stage="index")))
            else:
                self.documents[document["ChunkId"]] = document.copy()
        if self.crash:
            self.crash = False
            raise GlobalFailure("lost_acknowledgement", "Simulated failure after write", retryable=True, stage="index")
        return failures
    def close(self):
        pass


def pipeline(config, tokenizer, elastic):
    return Pipeline(config, tokenizer=tokenizer, elastic=elastic, extractor=Extractor(config), scheduler=ImmediateScheduler())


def test_pipeline_good_fallback_missing_and_zip_continue(config, tokenizer, tmp_path):
    import zipfile
    path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(path, "w") as output:
        output.writestr("one.txt", "word evidence")
        output.writestr("two.txt", "word other evidence")
        output.writestr("broken.bin", b"\x00\xff\x00")
    sources = [hit("1"), hit("2", text=""), hit("3", text="", path=str(path))]
    elastic = MemoryElastic(sources)
    summary = pipeline(config, tokenizer, elastic).run()
    assert summary["fallback"]["sources"] == 1
    assert summary["skipped"]["sources"] == 1
    assert summary["partial"]["sources"] == 1
    zip_docs = [d for d in elastic.documents.values() if d["DocId"] == "3"]
    assert len(zip_docs) == 2 and len({d["FileKey"] for d in zip_docs}) == 2
    assert all(not d["CoverageComplete"] and "unsupported_or_dependency_missing" in d["WarningCodes"] for d in zip_docs)


def test_resume_after_lost_bulk_ack_is_idempotent(config, tokenizer):
    elastic = MemoryElastic([hit("1", text="word " * 4000)], crash=True)
    with pytest.raises(GlobalFailure):
        pipeline(config, tokenizer, elastic).run()
    before = set(elastic.documents)
    assert before
    summary = pipeline(config, tokenizer, elastic).run()
    assert set(elastic.documents) == before
    assert summary["fallback"]["sources"] == 1
    state = State(config.path(config.runtime["state_path"]))
    assert state.meta("enumeration_complete") is True
    state.close()


def test_bulk_rejection_marks_successful_siblings_partial(config, tokenizer):
    elastic = MemoryElastic([hit("1", text="word " * 2500)], reject_ordinal=1)
    summary = pipeline(config, tokenizer, elastic).run()
    assert summary["partial"]["sources"] == 1
    assert all(not d["CoverageComplete"] and d["ProcessingStatus"] == "partial" for d in elastic.documents.values())


def test_completed_sources_are_not_embedded_again(config, tokenizer):
    elastic = MemoryElastic([hit("1")])
    pipeline(config, tokenizer, elastic).run()
    initial = dict(elastic.documents)
    second = pipeline(config, tokenizer, elastic).run()
    assert elastic.documents == initial and second["fallback"]["sources"] == 1


def test_changed_sources_require_new_build(config):
    state = State(config.path(config.runtime["state_path"]))
    source = hit("1")
    assert state.enqueue(source, "original", "run")
    state.finish(source_key(source), "complete", 1)
    with pytest.raises(ValueError, match="new versioned"):
        state.enqueue(source, "changed", "run2")
    state.close()


def test_state_reset_restores_running_job(config):
    state = State(config.path(config.runtime["state_path"]))
    source = hit()
    state.enqueue(source, "hash", "run")
    assert state.claim(source_key(source))
    state.close()
    state = State(config.path(config.runtime["state_path"]))
    state.reset_running()
    assert list(state.retained())[0]["_id"] == source["_id"]
    state.close()


def test_isolated_extraction_worker_process(config, tmp_path):
    import threading
    from reports_ingest.isolation import ExtractionPool
    from reports_ingest.extraction import Unit
    file = tmp_path / "note.txt"; file.write_text("word evidence", encoding="utf-8")
    pool = ExtractionPool(config, threading.Event())
    try:
        values = list(pool.record(hit(path=str(file), text="")))
        assert any(isinstance(v, Unit) and v.text == "word evidence" for v in values)
    finally:
        pool.close()

