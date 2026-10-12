from __future__ import annotations

import base64
import io
import math
import threading
import time
import wave
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from .common import GlobalFailure, IngestError
from .http import Http


@dataclass
class Input:
    modality: str
    context: dict
    prompt: str | None = None
    media: bytes | None = None
    mime: str | None = None
    tokens: int = 0

    @property
    def size(self):
        return len(self.media or b"") * 4 // 3 + len((self.prompt or "").encode("utf-8")) + 1024


def validate_vectors(response, count: int, dimensions: int):
    values = response.get("data", [])
    if len(values) != count:
        raise GlobalFailure("embedding_count_mismatch", "Embedding response must contain one vector per input", stage="embed")
    if any("index" not in v for v in values):
        raise GlobalFailure("embedding_order_missing", "Embedding response lacks input indices", stage="embed")
    ordered = sorted(values, key=lambda v: v["index"])
    if [v["index"] for v in ordered] != list(range(count)):
        raise GlobalFailure("embedding_order_mismatch", "Embedding response has duplicate/missing input indices", stage="embed")
    vectors = []
    for value in ordered:
        vector = value.get("embedding")
        if not isinstance(vector, list) or len(vector) != dimensions:
            raise GlobalFailure("embedding_dimension_mismatch", f"Expected {dimensions}-dimensional sequence embeddings", stage="embed")
        if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in vector):
            raise GlobalFailure("embedding_nonfinite", "Embedding contains nonnumeric/NaN/infinite values", stage="embed")
        norm = math.sqrt(sum(x * x for x in vector))
        if not math.isfinite(norm) or norm < 1e-12:
            raise GlobalFailure("embedding_zero_norm", "Embedding has invalid/zero norm", stage="embed")
        vectors.append([x / norm for x in vector])
    return vectors


class EmbeddingClient:
    def __init__(self, config, endpoint: str, http=None, events=None):
        self.config, self.settings, self.endpoint = config, config.embed, endpoint
        self.events = events
        key = config.secret(config.embed, "api_key_env")
        self.http = http or Http(endpoint, headers={"Authorization": f"Bearer {key}"} if key else {},
            verify=config.embed["verify_tls"], timeout=config.embed["timeout_seconds"], retries=config.embed["retries"], max_response_bytes=config.runtime.get("max_http_response_bytes", 67108864))

    def retry_callback(self, context):
        def callback(error, attempt, status):
            if self.events:
                self.events.error(context, error, "retry_scheduled", Attempt=attempt, Endpoint=self.endpoint, HttpStatus=status)
        return callback

    def embed(self, inputs: list[Input]):
        if not inputs:
            return []
        modality = inputs[0].modality
        if any(v.modality != modality for v in inputs):
            raise ValueError("Batch must contain one modality")
        if modality == "text":
            response = self.http.json("POST", "/v1/embeddings", json={"model": self.settings["model"],
                "input": [v.prompt for v in inputs], "encoding_format": "float"}, on_retry=self.retry_callback(inputs[0].context))
            return validate_vectors(response, len(inputs), self.settings["dimensions"])
        if modality == "image" and self.settings["media_transport"] == "cohere_images":
            response = self.http.json("POST", "/v2/embed", json={"model": self.settings["model"],
                "images": [self.data_uri(v) for v in inputs], "embedding_types": ["float"], "truncate": "NONE"}, on_retry=self.retry_callback(inputs[0].context))
            vectors = response.get("embeddings", {}).get("float", [])
            return validate_vectors({"data": [{"index": i, "embedding": v} for i, v in enumerate(vectors)]}, len(inputs), self.settings["dimensions"])
        # vLLM messages describe ONE multimodal sequence, not a batch of independent sequences.
        # Concurrent individual requests let vLLM continuously batch them without combining vectors.
        with ThreadPoolExecutor(max_workers=self.settings["media_concurrency"]) as executor:
            return list(executor.map(self.single_media, inputs))

    @staticmethod
    def data_uri(value: Input):
        return f"data:{value.mime};base64," + base64.b64encode(value.media).decode("ascii")

    def single_media(self, value: Input):
        if value.modality == "image":
            content = {"type": "image_url", "image_url": {"url": self.data_uri(value)}}
        else:
            content = {"type": "input_audio", "input_audio": {"data": base64.b64encode(value.media).decode("ascii"), "format": "wav"}}
        body = {"model": self.settings["model"], "messages": [{"role": "user", "content": [content]}],
            "encoding_format": "float", **self.settings.get("media_extra_body", {})}
        response = self.http.json("POST", self.settings["media_path"], json=body, on_retry=self.retry_callback(value.context))
        return validate_vectors(response, 1, self.settings["dimensions"])[0]

    def preflight(self):
        result = self.http.json("GET", "/v1/models")
        names = [m.get("id") for m in result.get("data", [])]
        if self.settings["model"] not in names:
            raise GlobalFailure("served_model_mismatch", "Configured model is not advertised by /v1/models", stage="preflight")
        checks = {}
        for modality in self.settings["modalities"]:
            if modality == "text":
                inputs = [Input("text", {}, prompt=f"title: synthetic | text: Sample {i} for retrieval.", tokens=20) for i in range(2)]
            elif modality == "image":
                from PIL import Image
                inputs = []
                for color in ("red", "blue"):
                    buffer = io.BytesIO()
                    Image.new("RGB", (64, 64), color).save(buffer, format="PNG")
                    inputs.append(Input("image", {}, media=buffer.getvalue(), mime="image/png", tokens=280))
            else:
                output = io.BytesIO()
                with wave.open(output, "wb") as wav:
                    wav.setnchannels(1); wav.setsampwidth(2); wav.setframerate(16000)
                    import struct
                    wav.writeframes(b"".join(struct.pack("<h", int(2000 * math.sin(2 * math.pi * 440 * i / 16000))) for i in range(16000)))
                inputs = [Input("audio", {}, media=output.getvalue(), mime="audio/wav", tokens=25)]
            try:
                vectors = self.embed(inputs)
                checks[modality] = {"vectors": len(vectors), "dimensions": len(vectors[0])}
            except IngestError as error:
                raise GlobalFailure("endpoint_preflight_failed", f"{modality} failed at {self.endpoint}: {error.code}; verify installed vLLM pooling/template/API", stage="preflight") from None
        return {"endpoint": self.endpoint, "model": self.settings["model"], "checks": checks}

    def close(self):
        self.http.close()


class Scheduler:
    def __init__(self, config, stop, events, clients=None):
        self.config, self.stop, self.events = config, stop, events
        self.clients = clients or [EmbeddingClient(config, url, events=events) for url in config.embed["endpoints"]]
        self.condition = threading.Condition()
        self.queues = {m: deque() for m in config.embed["modalities"]}
        self.closed = False
        self.fatal = None
        self.workers = [threading.Thread(target=self.worker, args=(i,), name=f"embed-{i}", daemon=True) for i in range(len(self.clients))]
        for worker in self.workers:
            worker.start()

    def submit(self, value: Input) -> Future:
        future = Future()
        if value.modality not in self.queues:
            future.set_exception(IngestError("modality_disabled", f"{value.modality} embedding is disabled", stage="embed"))
            return future
        if value.size > self.config.embed["batch_bytes"] or value.tokens > self.config.embed["batch_tokens"]:
            future.set_exception(IngestError("embedding_payload_limit", "One unit exceeds configured embedding payload/token budget", stage="embed"))
            return future
        with self.condition:
            while sum(map(len, self.queues.values())) >= self.config.embed["queue_items"] and not self.stop.is_set():
                self.condition.wait(0.25)
            if self.stop.is_set() or self.closed:
                future.set_exception(self.fatal or GlobalFailure("run_stopped", "Work retained for resume", retryable=True, stage="embed"))
            else:
                self.queues[value.modality].append((value, future))
            self.condition.notify_all()
        return future

    def worker(self, index):
        rotation = index
        while True:
            with self.condition:
                while not any(self.queues.values()) and not self.closed and not self.stop.is_set():
                    self.condition.wait(0.25)
                if self.stop.is_set():
                    for queue_ in self.queues.values():
                        while queue_:
                            _, future = queue_.popleft()
                            future.set_exception(self.fatal or GlobalFailure("run_stopped", "Work retained for resume", retryable=True, stage="embed"))
                    self.condition.notify_all()
                    return
                if not any(self.queues.values()) and self.closed:
                    return
                names = list(self.queues)
                modality = next(names[(rotation + i) % len(names)] for i in range(len(names)) if self.queues[names[(rotation + i) % len(names)]])
                rotation = (names.index(modality) + 1) % len(names)
                queue_ = self.queues[modality]
                count = self.config.embed[f"{modality}_batch"]
                # Let other extraction threads fill a batch; wait releases the condition lock.
                if len(queue_) < count and not self.closed:
                    self.condition.wait(0.03)
                batch, tokens, bytes_ = [], 0, 0
                while queue_ and len(batch) < count:
                    value = queue_[0][0]
                    if batch and (tokens + value.tokens > self.config.embed["batch_tokens"] or bytes_ + value.size > self.config.embed["batch_bytes"]):
                        break
                    batch.append(queue_.popleft())
                    tokens += value.tokens; bytes_ += value.size
                self.condition.notify_all()
            try:
                self.process(batch, index)
            except GlobalFailure as error:
                self.fatal = error
                self.stop.set()
                for _, future in batch:
                    if not future.done():
                        future.set_exception(error)
                with self.condition:
                    self.condition.notify_all()

    def process(self, batch, preferred):
        inputs = [value for value, _ in batch]
        try:
            try:
                vectors = self.clients[preferred].embed(inputs)
            except GlobalFailure as error:
                if error.code not in {"service_unreachable", "service_unavailable"}:
                    raise
                other = [c for i, c in enumerate(self.clients) if i != preferred]
                for client in other:
                    self.events.error(inputs[0].context, error, "endpoint_failover", Endpoint=self.clients[preferred].endpoint)
                    try:
                        vectors = client.embed(inputs)
                        break
                    except GlobalFailure as next_error:
                        error = next_error
                else:
                    raise error
            for (_, future), vector in zip(batch, vectors):
                future.set_result(vector)
        except GlobalFailure:
            raise
        except IngestError as error:
            if len(batch) > 1:
                self.events.error(inputs[0].context, error, "batch_split", Endpoint=self.clients[preferred].endpoint)
                middle = len(batch) // 2
                self.process(batch[:middle], preferred)
                self.process(batch[middle:], preferred)
            else:
                batch[0][1].set_exception(error)
        except Exception:
            raise GlobalFailure("embedding_worker_failed", "Unexpected embedding worker failure; pending jobs retained", stage="embed") from None

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        for thread in self.workers:
            thread.join()
        for client in self.clients:
            client.close()

