from __future__ import annotations

import json
import time
from urllib.parse import quote

from .common import GlobalFailure, IngestError, canonical
from .http import Http
from .schema import index_schema


class Elastic:
    def __init__(self, config, http=None):
        self.config, self.settings = config, config.es
        self.events = None
        key = config.secret(config.es, "api_key_env")
        headers = {"Authorization": f"ApiKey {key}"} if key else {}
        password = config.secret(config.es, "password_env")
        auth = (config.es["username"], password) if not key and password else None
        self.http = http or Http(config.es["url"], auth=auth, headers=headers, verify=config.es["verify_tls"], retries=config.runtime["http_retries"], max_response_bytes=config.runtime.get("max_http_response_bytes", 67108864))

    def inspect(self):
        info = self.http.json("GET", "/")
        major = int(info["version"]["number"].split(".")[0])
        if major < 8:
            raise GlobalFailure("unsupported_es_version", "Elasticsearch 8+ with indexed dense_vector is required", stage="preflight")
        mappings, counts = {}, {}
        for name in self.settings["source_indices"]:
            mappings[name] = self.http.json("GET", f"/{quote(name)}/_mapping")
            if any(v.get("mappings", {}).get("_source", {}).get("enabled") is False for v in mappings[name].values()):
                raise GlobalFailure("source_disabled", f"{name} has disabled _source", stage="preflight")
            counts[name] = self.http.json("GET", f"/{quote(name)}/_count", json={"query": self.settings["query"]})["count"]
        return {"version": info["version"]["number"], "mappings": mappings, "counts": counts}

    def ensure_index(self, create=False, build_id=None):
        name = quote(self.settings["target_index"])
        response = self.http.request("GET", f"/{name}/_mapping", allow_status=(404,))
        if response.status_code == 404:
            if not create:
                raise GlobalFailure("target_missing", "Target index does not exist; use ingest --create-index", stage="preflight")
            schema = index_schema(self.config)
            if build_id:
                schema["mappings"]["_meta"]["build_id"] = build_id
            self.http.json("PUT", f"/{name}", json=schema)
            return
        mapping = response.json()[self.settings["target_index"]]["mappings"]
        if mapping.get("_meta", {}).get("config_hash") != self.config.signature:
            raise GlobalFailure("index_contract_mismatch", "Target model/pipeline contract differs; use a new versioned index", stage="preflight")
        if build_id and mapping.get("_meta", {}).get("build_id") != build_id:
            raise GlobalFailure("build_identity_mismatch", "Target belongs to another state database/build; restore the matching state or use a new target", stage="preflight")
        expected = index_schema(self.config)["mappings"]["properties"]
        actual = mapping.get("properties", {})
        if any(actual.get(k, {}).get("type") != v["type"] for k, v in expected.items()):
            raise GlobalFailure("index_mapping_mismatch", "Target fields differ from ingestion mapping", stage="preflight")
        if actual["Embedding"].get("dims") != self.config.embed["dimensions"]:
            raise GlobalFailure("index_dimension_mismatch", "Target embedding dimensions differ", stage="preflight")

    def published(self):
        result = self.http.json("GET", f"/{quote(self.settings['target_index'])}/_alias", allow_status=(404,))
        return any(self.settings["read_alias"] in value.get("aliases", {}) for value in result.values() if isinstance(value, dict))

    def scan(self, index: str, limit=None, stop=None):
        emitted, restarts = 0, 0
        page_size = self.settings["page_size"]
        while True:
            pit = self.http.json("POST", f"/{quote(index)}/_pit", params={"keep_alive": self.settings["pit_keep_alive"]})["id"]
            after = None
            try:
                while not (stop and stop.is_set()):
                    body = {"size": page_size, "query": self.settings["query"], "sort": ["_shard_doc"],
                        "pit": {"id": pit, "keep_alive": self.settings["pit_keep_alive"]}, "track_total_hits": False, "seq_no_primary_term": True}
                    if after is not None:
                        body["search_after"] = after
                    if self.settings.get("scan_source_fields"):
                        body["_source"] = self.settings["scan_source_fields"]
                    try:
                        response = self.http.request("POST", "/_search", json=body, allow_status=(404,))
                    except IngestError as error:
                        if error.code == "response_size_limit" and page_size > 1:
                            page_size = max(1, page_size // 2)
                            continue
                        raise
                    if response.status_code == 404:
                        restarts += 1
                        if restarts > 3:
                            raise GlobalFailure("pit_expired", "PIT repeatedly expired; increase keep_alive or reduce queue pressure", retryable=True, stage="scan")
                        break  # Start a fresh PIT from its beginning; state deduplicates replayed jobs.
                    result = response.json()
                    pit = result.get("pit_id", pit)
                    if result.get("timed_out") or result.get("_shards", {}).get("failed", 0):
                        raise GlobalFailure("incomplete_search", "Source search timed out or has failed shards; no cursor advancement", retryable=True, stage="scan")
                    hits = result["hits"]["hits"]
                    if not hits:
                        return
                    for hit in hits:
                        yield hit
                        emitted += 1
                        if limit and emitted >= limit:
                            return
                    after = hits[-1]["sort"]
                else:
                    return
            finally:
                try:
                    self.http.request("DELETE", "/_pit", json={"id": pit}, allow_status=(404,))
                except IngestError:
                    pass

    def parents(self, ids):
        field = self.settings["parent_lookup_field"]
        result = self.http.json("POST", f"/{quote(self.settings['source_indices'][0])}/_search", json={
            "size": len(ids) * 2, "query": {"terms": {field: list(ids)}}, "_source": ["DocId", "Branch", "ReportType", "DocumentDate"], "track_total_hits": False})
        found = {}
        if result.get("timed_out") or result.get("_shards", {}).get("failed", 0):
            raise GlobalFailure("parent_lookup_incomplete", "Parent lookup failed shards or timed out", retryable=True, stage="enrich")
        for hit in result["hits"]["hits"]:
            doc_id = str(hit["_source"].get("DocId", ""))
            if doc_id in found:
                raise GlobalFailure("duplicate_parent_id", "Parent DocId uniqueness violated", stage="enrich")
            found[doc_id] = hit
        return found

    def bulk(self, documents: list[dict]):
        remaining = documents
        failures = []
        for attempt in range(self.config.runtime["http_retries"] + 1):
            payload = "".join(canonical({"index": {"_index": self.settings["target_index"], "_id": d["ChunkId"]}}) + "\n" + canonical(d) + "\n" for d in remaining)
            def retry_event(error, number, status):
                if self.events:
                    self.events.error(remaining[0], error, "retry_scheduled", Attempt=number, Endpoint=self.http.base, HttpStatus=status)
            result = self.http.json("POST", "/_bulk", content=payload.encode("utf-8"), headers={"Content-Type": "application/x-ndjson"}, on_retry=retry_event)
            items = result.get("items", [])
            if len(items) != len(remaining):
                raise GlobalFailure("bulk_response_count", "Bulk response item count mismatch; source remains pending", retryable=True, stage="index")
            retry = []
            for document, item in zip(remaining, items):
                value = item.get("index", {})
                status = value.get("status", 500)
                if status < 300 and not value.get("error"):
                    continue
                if status in {401, 403, 404}:
                    raise GlobalFailure("target_unavailable", f"Bulk target/authorization failure HTTP {status}", stage="index")
                if status in {408, 429, 500, 502, 503, 504}:
                    if self.events:
                        self.events.error(document, IngestError("bulk_item_retry", f"Transient bulk item HTTP {status}", retryable=True, stage="index"), "retry_scheduled", Attempt=attempt + 1, HttpStatus=status)
                    retry.append(document)
                else:
                    kind = value.get("error", {}).get("type", "bulk_item_rejected")
                    failures.append((document, IngestError(kind, f"Elasticsearch rejected chunk (HTTP {status}, {kind}); inspect mapping and unit", stage="index")))
            if not retry:
                return failures
            remaining = retry
            if attempt == self.config.runtime["http_retries"]:
                raise GlobalFailure("bulk_retry_exhausted", "Elasticsearch remains overloaded/unavailable; unacknowledged source retained", retryable=True, stage="index")
            time.sleep(min(8, 0.5 * 2 ** attempt))
        return failures

    def publish(self):
        target, alias = self.settings["target_index"], self.settings["read_alias"]
        old = self.http.json("GET", f"/_alias/{quote(alias)}", allow_status=(404,))
        actions = [{"remove": {"index": name, "alias": alias}} for name, value in old.items()
            if isinstance(value, dict) and alias in value.get("aliases", {})]
        actions.append({"add": {"index": target, "alias": alias}})
        self.http.json("POST", f"/{quote(target)}/_refresh")
        self.http.json("POST", "/_aliases", json={"actions": actions})

    def close(self):
        self.http.close()

