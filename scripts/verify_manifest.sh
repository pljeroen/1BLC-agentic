#!/usr/bin/env bash
set -euo pipefail

MANIFEST="${1:-}"

if [[ -z "$MANIFEST" || ! -f "$MANIFEST" ]]; then
  echo "Usage: $0 <manifest.json>" >&2
  exit 1
fi

python3 - <<'PY' "$MANIFEST"
import hashlib
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1]).resolve()
m = json.loads(manifest_path.read_text())
base = manifest_path.parent

required = [
    "schema_version",
    "input_file",
    "input_sha256",
    "jar_file",
    "jar_sha256",
    "output_file",
    "output_sha256",
    "stdout_file",
    "stdout_sha256",
    "stderr_file",
    "stderr_sha256",
]
for k in required:
    if k not in m:
        raise SystemExit(f"Missing required key: {k}")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

checks = [
    (Path(m["input_file"]), m["input_sha256"], "input"),
    (Path(m["jar_file"]), m["jar_sha256"], "jar"),
    (base / m["output_file"], m["output_sha256"], "output"),
    (base / m["stdout_file"], m["stdout_sha256"], "stdout"),
    (base / m["stderr_file"], m["stderr_sha256"], "stderr"),
]

for path, expected, label in checks:
    if not path.exists():
        raise SystemExit(f"Missing {label} artifact: {path}")
    actual = sha256_file(path)
    if actual != expected:
        raise SystemExit(f"Hash mismatch for {label}: expected {expected}, got {actual}")

prev = m.get("prev_manifest_sha256")
if prev:
    prev_file = base / m.get("prev_manifest_file", "")
    if not prev_file.exists():
        raise SystemExit(f"Previous manifest missing: {prev_file}")
    prev_actual = sha256_file(prev_file)
    if prev_actual != prev:
        raise SystemExit(
            f"Previous manifest hash mismatch: expected {prev}, got {prev_actual}"
        )

print("Manifest verification passed")
PY
