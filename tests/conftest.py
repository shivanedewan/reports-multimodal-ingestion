import json
from pathlib import Path

import pytest

from reports_ingest.config import Config
from reports_ingest.chunking import Tokenizer


@pytest.fixture
def config(tmp_path):
    data = json.loads((Path(__file__).parents[1] / "config.example.json").read_text())
    data["embedding"].update(model_revision="test-immutable", tokenizer_revision="test-immutable", modalities=["text", "image", "audio"], retries=0)
    data["extraction"].update(workers=2, max_pending_jobs=4, max_pending_chunks_per_source=4)
    data["runtime"].update(min_free_disk_bytes=0)
    for key in ("directory", "temp_path", "text_cache_path"):
        (tmp_path / data["runtime"][key]).mkdir(parents=True, exist_ok=True)
    return Config(data, tmp_path)


@pytest.fixture
def tokenizer(config):
    from tokenizers import Tokenizer as Backend, models, pre_tokenizers
    backend = Backend(models.WordLevel({"[UNK]": 0, "title": 1, "text": 2, "none": 3, "word": 4, ":": 5, "|": 6}, unk_token="[UNK]"))
    backend.pre_tokenizer = pre_tokenizers.Whitespace()
    path = config.path(config.embed["tokenizer_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    backend.save(str(path))
    return Tokenizer(path)


def hit(doc="1", text="Useful searchable evidence.", path="", index="reports_main_safe", **extra):
    return {"_index": index, "_id": "es-" + doc, "_source": {"DocId": doc, "Text": text,
        "SystemPath": path, "OriginalName": Path(path).name or "missing.txt", **extra}}

