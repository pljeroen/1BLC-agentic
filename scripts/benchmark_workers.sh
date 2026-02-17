#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-measurements.txt}"
RUNS="${RUNS:-5}"
WARMUPS="${WARMUPS:-1}"
MAX_WORKERS="${MAX_WORKERS:-$(nproc)}"
TIME_TO_BEAT_MS="${TIME_TO_BEAT_MS:-1535}"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

if (( MAX_WORKERS < 1 )); then
  echo "MAX_WORKERS must be >= 1" >&2
  exit 1
fi

workers=(1)
w=2
while (( w <= MAX_WORKERS )); do
  workers+=("$w")
  w=$((w * 2))
done
if [[ " ${workers[*]} " != *" $MAX_WORKERS "* ]]; then
  workers+=("$MAX_WORKERS")
fi

summary_paths=()
for wk in "${workers[@]}"; do
  echo "== worker sweep: workers=$wk =="
  out="$(RUNS="$RUNS" WARMUPS="$WARMUPS" WORKERS="$wk" TIME_TO_BEAT_MS="$TIME_TO_BEAT_MS" ./scripts/benchmark.sh "$INPUT_FILE")"
  echo "$out"
  dir="$(printf '%s\n' "$out" | awk -F': ' '/Benchmark artifacts:/ {print $2}' | tail -n1)"
  if [[ -z "$dir" || ! -f "$dir/summary.json" ]]; then
    echo "Failed to locate benchmark summary for workers=$wk" >&2
    exit 2
  fi
  summary_paths+=("$dir/summary.json")
done

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_file="proofs/worker-sweep-$ts.json"

python3 - <<'PY' "$out_file" "${summary_paths[@]}"
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
entries = []
for p in sys.argv[2:]:
    data = json.loads(Path(p).read_text())
    entries.append({
        "workers": data["workers"],
        "best_ms": data["best_ms"],
        "median_ms": data["median_ms"],
        "p90_ms": data["p90_ms"],
        "time_to_beat_ms": data["time_to_beat_ms"],
        "delta_median_ms": data["delta_median_ms"],
        "delta_median_percent": data["delta_median_percent"],
        "summary_path": p,
    })

entries.sort(key=lambda e: (e["median_ms"], e["p90_ms"], e["workers"]))
best = entries[0]

result = {
    "schema_version": "1.0",
    "recommended_workers": best["workers"],
    "selection_rule": "min(median_ms), tie-break on p90_ms then workers",
    "results": entries,
}
out.write_text(json.dumps(result, indent=2) + "\n")

print(json.dumps(result, indent=2))
PY

echo "Worker sweep report: $out_file"
