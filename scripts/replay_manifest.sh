#!/usr/bin/env bash
set -euo pipefail

MANIFEST="${1:-}"

if [[ -z "$MANIFEST" || ! -f "$MANIFEST" ]]; then
  echo "Usage: $0 <manifest.json>" >&2
  exit 1
fi

./scripts/verify_manifest.sh "$MANIFEST"

python3 - <<'PY' "$MANIFEST"
import hashlib
import json
import shlex
import subprocess
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1]).resolve()
manifest = json.loads(manifest_path.read_text())
base = manifest_path.parent

cmd = shlex.split(manifest["command"])
rerun_out = base / (manifest_path.stem + ".replay.out")
rerun_err = base / (manifest_path.stem + ".replay.err")

with rerun_out.open("wb") as out, rerun_err.open("wb") as err:
    proc = subprocess.run(cmd, stdout=out, stderr=err, check=False)

if proc.returncode != int(manifest.get("exit_code", 0)):
    raise SystemExit(f"Replay exit code mismatch: {proc.returncode}")

h = hashlib.sha256(rerun_out.read_bytes()).hexdigest()
if h != manifest["output_sha256"]:
    raise SystemExit(
        f"Replay output hash mismatch: expected {manifest['output_sha256']}, got {h}"
    )

print("Replay verification passed")
print(f"replay_output={rerun_out}")
print(f"replay_error={rerun_err}")
PY
