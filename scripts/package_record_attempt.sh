#!/usr/bin/env bash
set -euo pipefail

BENCH_SUMMARY="${1:-}"
MANIFEST="${2:-}"
OUT_DIR="${3:-proofs/record-attempt-$(date -u +%Y%m%dT%H%M%SZ)}"

if [[ -z "$BENCH_SUMMARY" || -z "$MANIFEST" ]]; then
  echo "Usage: $0 <benchmark-summary.json> <run-manifest.json> [out_dir]" >&2
  exit 1
fi

if [[ ! -f "$BENCH_SUMMARY" ]]; then
  echo "Missing benchmark summary: $BENCH_SUMMARY" >&2
  exit 1
fi
if [[ ! -f "$MANIFEST" ]]; then
  echo "Missing run manifest: $MANIFEST" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
cp "$BENCH_SUMMARY" "$OUT_DIR/"
cp "$MANIFEST" "$OUT_DIR/"

python3 - <<'PY' "$BENCH_SUMMARY" "$MANIFEST" "$OUT_DIR"
import json
import sys
from pathlib import Path

bench = json.loads(Path(sys.argv[1]).read_text())
man = json.loads(Path(sys.argv[2]).read_text())
out = Path(sys.argv[3])

report = {
    "schema_version": "1.0",
    "claim_manifest": Path(sys.argv[2]).name,
    "benchmark_summary": Path(sys.argv[1]).name,
    "best_ms": bench["best_ms"],
    "median_ms": bench["median_ms"],
    "p90_ms": bench["p90_ms"],
    "time_to_beat_ms": bench["time_to_beat_ms"],
    "delta_best_ms": bench["delta_best_ms"],
    "delta_best_percent": bench["delta_best_percent"],
    "output_sha256": man["output_sha256"],
    "command": man["command"],
}

(out / "record-attempt-report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
PY

echo "Record attempt package written to: $OUT_DIR"
