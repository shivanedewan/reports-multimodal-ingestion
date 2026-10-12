from __future__ import annotations

import random
import time
from contextlib import closing

import httpx

from .common import GlobalFailure, IngestError


class Http:
    def __init__(self, base: str, *, auth=None, headers=None, verify=True, timeout=120, retries=3, transport=None, max_response_bytes=67108864):
        self.base = base.rstrip("/")
        self.retries = retries
        self.max_response_bytes = max_response_bytes
        self.client = httpx.Client(base_url=self.base, auth=auth, headers=headers, verify=verify,
            timeout=timeout, trust_env=False, transport=transport)

    def request(self, method: str, path: str, *, allow_status=(), on_retry=None, **kwargs):
        for attempt in range(self.retries + 1):
            try:
                request = self.client.build_request(method, path, **kwargs)
                with closing(self.client.send(request, stream=True)) as streamed:
                    payload = bytearray()
                    for block in streamed.iter_bytes():
                        if len(payload) + len(block) > self.max_response_bytes:
                            raise IngestError("response_size_limit", "HTTP response exceeds configured decoded byte budget", stage="http")
                        payload.extend(block)
                    headers = {k: v for k, v in streamed.headers.items() if k.lower() not in {"content-encoding", "content-length"}}
                    response = httpx.Response(streamed.status_code, headers=headers, content=bytes(payload), request=request)
            except httpx.TransportError:
                if attempt == self.retries:
                    raise GlobalFailure("service_unreachable", f"Cannot reach {self.base}; pending work retained", retryable=True, stage="http") from None
                if on_retry:
                    on_retry(IngestError("transport_retry", "Transient transport failure", retryable=True, stage="http"), attempt + 1, None)
            else:
                if response.status_code < 400 or response.status_code in allow_status:
                    return response
                if response.status_code in {401, 403}:
                    raise GlobalFailure("service_authorization", f"HTTP {response.status_code} at {self.base}; verify credentials/permissions", stage="http")
                if response.status_code not in {408, 429, 500, 502, 503, 504}:
                    raise IngestError(f"http_{response.status_code}", f"HTTP {response.status_code} at {self.base}{path}", stage="http")
                if attempt == self.retries:
                    raise GlobalFailure("service_unavailable", f"HTTP {response.status_code} at {self.base}; retry budget exhausted", retryable=True, stage="http")
                if on_retry:
                    on_retry(IngestError("http_retry", f"Transient HTTP {response.status_code}", retryable=True, stage="http"), attempt + 1, response.status_code)
            time.sleep(min(8, 0.5 * 2 ** attempt) + random.uniform(0, 0.2))

    def json(self, method: str, path: str, **kwargs):
        response = self.request(method, path, **kwargs)
        try:
            return response.json()
        except ValueError:
            raise GlobalFailure("invalid_json_response", f"Invalid JSON from {self.base}{path}", stage="http") from None

    def close(self):
        self.client.close()

