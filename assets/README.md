# Deployment tokenizer

Place the exact deployed EmbeddingGemma 2 revision's `tokenizer.json` here. Set its immutable revision in `config.local.json`. The ingestion client requires no model weights and never downloads a tokenizer implicitly.

If the revision is available through Hugging Face, an operator can use:

```bash
hf download google/embeddinggemma-2 tokenizer.json --revision YOUR_DEPLOYED_COMMIT --local-dir assets
```

For an offline deployment, copy `tokenizer.json` from the same model snapshot used by both vLLM servers. Preflight checks the file and records the build contract; server/model revision alignment remains an operator requirement.

