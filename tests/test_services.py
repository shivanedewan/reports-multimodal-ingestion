import json
import threading
from concurrent.futures import Future

import httpx
import pytest

from reports_ingest.common import GlobalFailure, IngestError
from reports_ingest.elastic import Elastic
from reports_ingest.embeddings import EmbeddingClient, Input, Scheduler, validate_vectors
from reports_ingest.events import Events
from reports_ingest.http import Http


def test_embedding_response_indices_restore_order_and_normalize():
    result = validate_vectors({"data": [{"index": 1, "embedding": [0, 4]}, {"index": 0, "embedding": [3, 0]}]}, 2, 2)
    assert result == [[1, 0], [0, 1]]


@pytest.mark.parametrize("data,code", [
    ([{"index": 0, "embedding": [0, 0]}], "embedding_zero_norm"),
    ([{"index": 0, "embedding": [float('nan'), 1]}], "embedding_nonfinite"),
    ([{"index": 0, "embedding": [1]}], "embedding_dimension_mismatch"),
    ([{"index": 2, "embedding": [1, 2]}], "embedding_order_mismatch"),
    ([], "embedding_count_mismatch")])
def test_invalid_vectors_fail_closed(data, code):
    with pytest.raises(GlobalFailure) as error:
        validate_vectors({"data": data}, 1, 2)
    assert error.value.code == code


def test_media_messages_are_independent_requests(config):
    requests = []
    def respond(request):
        body = json.loads(request.content)
        requests.append(body)
        return httpx.Response(200, json={"data": [{"index": 0, "embedding": [1.] * 768}]})
    http = Http("http://test", retries=0, transport=httpx.MockTransport(respond))
    client = EmbeddingClient(config, "http://test", http)
    result = client.embed([Input("image", {}, media=b"png", mime="image/png") for _ in range(3)])
    assert len(result) == 3 and len(requests) == 3
    assert all(len(r["messages"][0]["content"]) == 1 for r in requests)
    client.close()


def test_bulk_retries_only_failed_items(config, monkeypatch):
    monkeypatch.setattr("reports_ingest.elastic.time.sleep", lambda _: None)
    calls = []
    def respond(request):
        rows = request.content.decode().strip().splitlines()
        ids = [json.loads(rows[i])["index"]["_id"] for i in range(0, len(rows), 2)]
        calls.append(ids)
        statuses = [201, 429] if len(calls) == 1 else [201]
        return httpx.Response(200, json={"items": [{"index": {"status": s}} for s in statuses]})
    elastic = Elastic(config, Http("http://test", retries=0, transport=httpx.MockTransport(respond)))
    assert elastic.bulk([{"ChunkId": "one"}, {"ChunkId": "two"}]) == []
    assert calls == [["one", "two"], ["two"]]
    elastic.close()


def test_bulk_mapping_rejection_is_item_specific(config):
    def respond(_):
        return httpx.Response(200, json={"items": [{"index": {"status": 201}}, {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}}]})
    elastic = Elastic(config, Http("http://test", transport=httpx.MockTransport(respond)))
    failures = elastic.bulk([{"ChunkId": "one"}, {"ChunkId": "two"}])
    assert len(failures) == 1 and failures[0][0]["ChunkId"] == "two"
    assert failures[0][1].code == "mapper_parsing_exception"
    elastic.close()


class FakeClient:
    endpoint = "http://fake"
    def __init__(self, outage=False):
        self.calls = []; self.outage = outage
    def embed(self, values):
        self.calls.append(values)
        if self.outage:
            raise GlobalFailure("service_unreachable", "offline", retryable=True)
        if any(v.prompt == "bad" for v in values):
            raise IngestError("http_400", "one bad input", stage="embed")
        return [[1.] + [0.] * 767 for _ in values]
    def close(self):
        pass


def test_scheduler_splits_bad_input_preserving_good_inputs(config):
    stop = threading.Event()
    client = FakeClient()
    scheduler = Scheduler(config, stop, Events(config.path(config.runtime["directory"]), "test"), [client])
    futures = [scheduler.submit(Input("text", {}, prompt=value, tokens=1)) for value in ("good", "bad", "also good")]
    assert futures[0].result(timeout=5)[0] == 1
    with pytest.raises(IngestError):
        futures[1].result(timeout=5)
    assert futures[2].result(timeout=5)[0] == 1
    scheduler.close()
    assert not stop.is_set()


def test_endpoint_failure_fails_over(config):
    clients = [FakeClient(outage=True), FakeClient()]
    scheduler = Scheduler(config, threading.Event(), Events(config.path(config.runtime["directory"]), "test"), clients)
    future = scheduler.submit(Input("text", {}, prompt="good", tokens=1))
    assert future.result(timeout=5)[0] == 1
    scheduler.close()


def test_pit_expiry_starts_with_fresh_cursor(config):
    calls = []
    search_count = 0
    def respond(request):
        nonlocal search_count
        if request.url.path.endswith("/_pit") and request.method == "POST":
            return httpx.Response(200, json={"id": "fresh-" + str(search_count)})
        if request.method == "DELETE":
            return httpx.Response(200, json={"succeeded": True})
        body = json.loads(request.content)
        calls.append(body)
        search_count += 1
        if search_count == 1:
            return httpx.Response(200, json={"hits": {"hits": [{"_index": "reports_main_safe", "_id": "a", "_source": {}, "sort": [10]}]}})
        if search_count == 2:
            return httpx.Response(404, json={"error": "expired"})
        return httpx.Response(200, json={"hits": {"hits": []}})
    elastic = Elastic(config, Http("http://test", retries=0, transport=httpx.MockTransport(respond)))
    assert len(list(elastic.scan("reports_main_safe"))) == 1
    assert "search_after" in calls[1] and "search_after" not in calls[2]
    elastic.close()

