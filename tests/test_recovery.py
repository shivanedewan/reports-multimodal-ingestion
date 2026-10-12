import threading

import httpx
import pytest

from reports_ingest.common import GlobalFailure, IngestError
from reports_ingest.elastic import Elastic
from reports_ingest.extraction import Notice, Unit
from reports_ingest.http import Http
from reports_ingest.locking import RunLock
from reports_ingest.pipeline import Pipeline
from reports_ingest.schema import index_schema
from .conftest import hit
from .test_pipeline import ImmediateScheduler, MemoryElastic


def test_retry_newly_successful_member_does_not_shift_existing_chunk_identity(config, tokenizer):
    class RecoveringExtractor:
        recovered = False
        def record(self, hit):
            if self.recovered:
                yield Unit("text", {"MemberId": "a", "MemberPath": "a.txt", "ContentSource": "archive_member"}, text="First recovered member")
            else:
                yield Notice("temporary_member_failure", "First member could not be read")
            yield Unit("text", {"MemberId": "b", "MemberPath": "b.txt", "ContentSource": "archive_member"}, text="Second good member")
    extractor = RecoveringExtractor()
    elastic = MemoryElastic([hit(text="")])
    def run(retry=False):
        return Pipeline(config, tokenizer=tokenizer, elastic=elastic, extractor=extractor, scheduler=ImmediateScheduler()).run(retry_failures=retry)
    run()
    first_ids = set(elastic.documents)
    extractor.recovered = True
    run(retry=True)
    assert first_ids.issubset(elastic.documents)
    assert len(elastic.documents) == 2


def test_parser_process_failure_can_use_indexed_text(config, tokenizer):
    class Crash:
        def record(self, hit):
            raise IngestError("parser_process_crashed", "Native decoder crashed")
            yield
    elastic = MemoryElastic([hit(text="Existing text still searchable")])
    summary = Pipeline(config, tokenizer=tokenizer, elastic=elastic, extractor=Crash(), scheduler=ImmediateScheduler()).run()
    assert summary["fallback"]["sources"] == 1
    assert next(iter(elastic.documents.values()))["UsedTextFallback"]


def test_single_process_lock(config):
    with RunLock(config.path(config.runtime["state_path"])):
        with pytest.raises(GlobalFailure, match="Another ingestion"):
            with RunLock(config.path(config.runtime["state_path"])):
                pass


def test_target_and_state_build_id_must_match(config, tokenizer):
    schema = index_schema(config)
    schema["mappings"]["_meta"]["build_id"] = "original-state"
    http = Http("http://test", transport=httpx.MockTransport(lambda _: httpx.Response(200, json={config.es["target_index"]: schema})))
    elastic = Elastic(config, http)
    with pytest.raises(GlobalFailure) as error:
        elastic.ensure_index(build_id="another-state")
    assert error.value.code == "build_identity_mismatch"
    elastic.close()


def test_http_response_byte_budget(config):
    http = Http("http://test", max_response_bytes=10, transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 100)))
    with pytest.raises(IngestError) as error:
        http.request("GET", "/")
    assert error.value.code == "response_size_limit"
    http.close()


def test_transient_http_retry_is_observable(monkeypatch):
    monkeypatch.setattr("reports_ingest.http.time.sleep", lambda _: None)
    calls = []; events = []
    def response(_):
        calls.append(1)
        return httpx.Response(429 if len(calls) == 1 else 200, json={"ok": True})
    http = Http("http://test", retries=1, transport=httpx.MockTransport(response))
    assert http.json("GET", "/", on_retry=lambda error, attempt, status: events.append((error.code, attempt, status))) == {"ok": True}
    assert events == [("http_retry", 1, 429)]
    http.close()


def test_parent_defaults_do_not_overwrite_child_or_inherit_acl(config):
    from reports_ingest.pipeline import source_metadata
    child = hit(index="reports_attachment_safe", Branch="Child branch", ReportType="", ParentDocId="12")
    child["_parent"] = hit("12", Branch="Parent branch", ReportType="Meeting", AllowedPrincipals=["administrator"])
    result = source_metadata(child, config)
    assert result["Branch"] == "Child branch" and result["ReportType"] == "Meeting"
    assert result["MetadataInheritedFields"] == ["ReportType"]
    assert "AllowedPrincipals" not in result


def test_generated_documents_match_explicit_schema(config, tokenizer):
    from reports_ingest.extraction import Extractor
    elastic = MemoryElastic([hit()])
    Pipeline(config, tokenizer=tokenizer, elastic=elastic, extractor=Extractor(config), scheduler=ImmediateScheduler()).run()
    properties = set(index_schema(config)["mappings"]["properties"])
    assert all(set(document) <= properties for document in elastic.documents.values())


def test_inventory_is_exhaustive_and_retains_missing_extensions(config):
    import csv
    from reports_ingest.cli import inventory
    sources = [hit("1", FileExtension="PDF"), hit("2", FileExtension=".pdf"), hit("3", OriginalName="README", FileExtension="")]
    inventory(config, MemoryElastic(sources))
    with (config.path(config.runtime["directory"]) / "file_type_inventory.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    assert sum(int(r["Records"]) for r in rows) == 3
    assert any(r["NormalizedExtension"] == "[missing]" for r in rows)
    assert all(r["Scope"] == "full_stream" for r in rows)

