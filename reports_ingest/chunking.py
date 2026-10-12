from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .common import IngestError, normalized_text


@dataclass
class TextChunk:
    text: str
    prompt: str
    start: int
    end: int
    tokens: int


class Tokenizer:
    def __init__(self, path: Path):
        from tokenizers import Tokenizer as Backend
        self.backend = Backend.from_file(str(path))
        # Never silently truncate source content based on tokenizer.json settings.
        self.backend.no_truncation()
        self.backend.no_padding()

    def offsets(self, text: str):
        return self.backend.encode(text, add_special_tokens=False).offsets

    def count(self, text: str) -> int:
        return len(self.backend.encode(text, add_special_tokens=True).ids)


class Chunker:
    def __init__(self, tokenizer, settings: dict):
        self.tokenizer, self.settings = tokenizer, settings

    def chunks(self, text: str, title: str = ""):
        text = normalized_text(text)
        if not text:
            return
        # Bound title cost too. Document content is never truncated to fit a long title.
        title = re.sub(r"\s+", " ", title).strip() or "none"
        while self.tokenizer.count(f"title: {title} | text: ") > 120 and len(title) > 1:
            title = title[:max(1, len(title) // 2)]
        prefix = f"title: {title} | text: "
        offsets = [(a, b) for a, b in self.tokenizer.offsets(text) if b > a]
        if not offsets:
            raise IngestError("tokenization_empty", "Nonempty text produced no content tokens", stage="chunk")
        boundaries = [m.end() for m in re.finditer(r"\n\s*\n|(?<=[.!?。！？])\s+", text)]
        target, overlap, hard = (self.settings[k] for k in ("target_tokens", "overlap_tokens", "max_input_tokens"))
        start_token = 0
        covered_end = 0
        while start_token < len(offsets):
            finish = min(len(offsets), start_token + target)
            begin_char = offsets[start_token][0]
            if start_token == 0:
                begin_char = 0
            end_char = offsets[finish - 1][1]
            if finish == len(offsets):
                end_char = len(text)
            # Prefer a boundary in the final quarter; ensure remaining content is not dropped.
            if finish < len(offsets):
                threshold = offsets[min(finish - 1, start_token + int(target * 0.75))][0]
                candidates = [b for b in boundaries if threshold <= b <= end_char]
                if candidates:
                    end_char = candidates[-1]
                    while finish > start_token + 1 and offsets[finish - 1][0] >= end_char:
                        finish -= 1
            body = text[begin_char:end_char]
            # Context-sensitive tokenization can differ from token counts on the complete string.
            while self.tokenizer.count(prefix + body) > hard:
                if finish <= start_token + 1:
                    raise IngestError("token_limit", "One content token exceeds input budget", stage="chunk")
                finish -= 1
                end_char = offsets[finish - 1][1]
                body = text[begin_char:end_char]
            if end_char <= covered_end:
                start_token += 1
                continue
            yield TextChunk(body, prefix + body, begin_char, end_char, self.tokenizer.count(prefix + body))
            covered_end = end_char
            if end_char >= len(text):
                break
            next_start = max(start_token + 1, finish - min(overlap, max(0, finish - start_token - 1)))
            # Several tokenizer tokens can refer to the same Unicode character.
            while next_start < finish and offsets[next_start][0] == offsets[start_token][0]:
                next_start += 1
            start_token = next_start

