from __future__ import annotations

import multiprocessing
import queue
import threading
import time

from .common import GlobalFailure, IngestError
from .config import Config
from .extraction import Extractor, Notice


def _worker(connection, data, base):
    config = Config(data, base)
    try:
        import resource
        budget = config.extract.get("worker_memory_bytes", 0)
        if budget:
            resource.setrlimit(resource.RLIMIT_AS, (budget, budget))
    except ImportError:
        pass  # Windows lacks POSIX rlimits; still uses process/time/byte/pixel isolation.
    extractor = Extractor(config)
    try:
        while True:
            hit = connection.recv()
            if hit is None:
                return
            try:
                for item in extractor.record(hit):
                    connection.send(("item", item))
                    # Bound interprocess buffering: main process acknowledges one item at a time.
                    if connection.recv() != "next":
                        return
            except Exception as error:
                connection.send(("error", (getattr(error, "code", "extractor_failed"), type(error).__name__, isinstance(error, GlobalFailure))))
            else:
                connection.send(("done", None))
    except (EOFError, BrokenPipeError):
        pass
    finally:
        connection.close()


class ExtractionPool:
    """Persistent subprocesses isolate native parser crashes; backpressure bounds media memory."""
    def __init__(self, config, stop: threading.Event):
        self.config, self.stop = config, stop
        self.context = multiprocessing.get_context("spawn")
        self.available = queue.Queue()
        self.slots = []
        self.closed = False
        self.lock = threading.Lock()
        for _ in range(config.extract["workers"]):
            slot = self._spawn()
            self.slots.append(slot)
            self.available.put(slot)

    def _spawn(self):
        parent, child = self.context.Pipe()
        process = self.context.Process(target=_worker, args=(child, self.config.data, self.config.base), daemon=True)
        process.start()
        child.close()
        return (process, parent)

    @staticmethod
    def _kill(slot):
        process, connection = slot
        connection.close()
        if process.is_alive():
            process.terminate()
        process.join(timeout=3)
        if process.is_alive():
            process.kill()
            process.join(timeout=3)

    def record(self, hit):
        while not self.stop.is_set():
            try:
                slot = self.available.get(timeout=0.25)
                break
            except queue.Empty:
                continue
        else:
            raise GlobalFailure("run_stopped", "Extraction stopped; source retained", retryable=True)
        process, connection = slot
        healthy = False
        try:
            connection.send(hit)
            timeout = self.config.extract["command_timeout_seconds"] + 30
            deadline = time.monotonic() + timeout
            while not self.stop.is_set():
                if connection.poll(0.25):
                    kind, value = connection.recv()
                    if kind == "done":
                        healthy = True
                        return
                    if kind == "error":
                        healthy = True
                        code, reason, global_error = value
                        if global_error:
                            raise GlobalFailure(code, reason)
                        yield Notice(code, reason)
                        return
                    yield value
                    if self.stop.is_set():
                        break
                    connection.send("next")
                    deadline = time.monotonic() + timeout
                elif not process.is_alive():
                    raise IngestError("parser_process_crashed", "Extractor process crashed; other sources continue")
                elif time.monotonic() >= deadline:
                    raise IngestError("parser_process_timeout", "Extractor did not produce a unit before its deadline")
            raise GlobalFailure("run_stopped", "Extraction stopped; source retained", retryable=True)
        except (EOFError, BrokenPipeError, OSError):
            raise IngestError("parser_process_crashed", "Extractor communication failed") from None
        finally:
            if not healthy:
                self._kill(slot)
                if not self.closed and not self.stop.is_set():
                    replacement = self._spawn()
                    with self.lock:
                        self.slots.remove(slot)
                        self.slots.append(replacement)
                    self.available.put(replacement)
            elif not self.closed:
                self.available.put(slot)

    def close(self):
        self.closed = True
        with self.lock:
            for slot in self.slots:
                self._kill(slot)

