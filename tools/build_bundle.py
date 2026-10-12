"""Build a portable source bundle from an explicit allowlist; exclude secrets/state/model assets."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import zipfile
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    freeze = subprocess.check_output([sys.executable, "-m", "pip", "freeze"], text=True)
    lines = [line for line in freeze.splitlines() if line and not line.startswith(("#", "-e ")) and not line.lower().startswith("reports-multimodal-ingestion")]
    (root / "requirements.lock.txt").write_text("# Tested environment snapshot; see pyproject.toml for portable core/optional dependencies.\n" + "\n".join(lines) + "\n", encoding="utf-8")
    files = [root / name for name in ("README.md", "INGESTION_PLAN.md", "config.example.json", "pyproject.toml", "requirements.lock.txt", ".gitignore", "assets/README.md")]
    for folder in ("reports_ingest", "tests", "tools"):
        files.extend(p for p in (root / folder).rglob("*.py") if "__pycache__" not in p.parts)
    destination = root / "dist" / "reports-ingestion.zip"
    destination.parent.mkdir(exist_ok=True)
    manifest = {str(path.relative_to(root)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(files):
            archive.write(path, str(path.relative_to(root)))
        archive.writestr("manifest.json", json.dumps(manifest, indent=2))
    with zipfile.ZipFile(destination) as archive:
        assert archive.testzip() is None
        assert len(archive.namelist()) == len(files) + 1
        assert all(not name.endswith(".env") and "runtime/" not in name and "venv/" not in name for name in archive.namelist())
    print(json.dumps({"bundle": str(destination), "files": len(files), "bytes": destination.stat().st_size,
        "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}, indent=2))


if __name__ == "__main__":
    main()
