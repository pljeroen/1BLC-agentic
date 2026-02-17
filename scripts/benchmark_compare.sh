#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-measurements.txt}"
RUNS="${RUNS:-3}"
WARMUPS="${WARMUPS:-1}"
WORKERS="${WORKERS:-$(nproc)}"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

if [[ ! -f "../1brc-upstream/target/average-1.0.0-SNAPSHOT.jar" ]]; then
  echo "Missing upstream jar; building ../1brc-upstream" >&2
  (cd ../1brc-upstream && ./mvnw -q -DskipTests package)
fi

./mvnw -q -DskipTests package

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="proofs/compare-$ts"
mkdir -p "$out_dir"

read -r -a JAVA_OPTS_ARR <<< "${JAVA_OPTS:-}"

run_case() {
  local label="$1"
  local cmd_file="$out_dir/$label.cmd.txt"
  shift
  local -a cmd=("$@")

  printf '%q ' "${cmd[@]}" > "$cmd_file"
  printf '\n' >> "$cmd_file"

  local i
  for ((i=1; i<=WARMUPS; i++)); do
    "${cmd[@]}" > "$out_dir/$label-warmup-$i.out"
  done

  local -a timings=()
  for ((i=1; i<=RUNS; i++)); do
    local start_ns end_ns elapsed_ms
    start_ns="$(date +%s%N)"
    "${cmd[@]}" > "$out_dir/$label-run-$i.out"
    end_ns="$(date +%s%N)"
    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    timings+=("$elapsed_ms")
    printf "%s\n" "$elapsed_ms" > "$out_dir/$label-run-$i.ms"
  done

  printf "%s\n" "${timings[@]}" > "$out_dir/$label-timings_ms.txt"

  local ref_sha
  ref_sha="$(sha256sum "$out_dir/$label-run-1.out" | awk '{print $1}')"
  for ((i=2; i<=RUNS; i++)); do
    local sha
    sha="$(sha256sum "$out_dir/$label-run-$i.out" | awk '{print $1}')"
    if [[ "$sha" != "$ref_sha" ]]; then
      echo "Non-deterministic output detected for $label" >&2
      exit 2
    fi
  done

  printf '%s\n' "$ref_sha" > "$out_dir/$label-output.sha256"
}

run_case "baseline" \
  java "${JAVA_OPTS_ARR[@]}" \
  -cp ../1brc-upstream/target/average-1.0.0-SNAPSHOT.jar \
  dev.morling.onebrc.CalculateAverage_baseline

run_case "jeroen" \
  java "${JAVA_OPTS_ARR[@]}" \
  -cp target/challenge-entry-0.1.0-SNAPSHOT.jar \
  dev.morling.onebrc.CalculateAverage_jeroen \
  "$INPUT_FILE" \
  "$WORKERS"

python3 - <<'PY' "$out_dir" "$RUNS" "$WARMUPS" "$WORKERS" "$INPUT_FILE"
import json
import statistics
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
runs = int(sys.argv[2])
warmups = int(sys.argv[3])
workers = int(sys.argv[4])
input_file = sys.argv[5]

def load_case(label: str):
    timings = [int(x.strip()) for x in (out_dir / f"{label}-timings_ms.txt").read_text().splitlines() if x.strip()]
    values_sorted = sorted(timings)
    p90_idx = max(0, int(len(values_sorted) * 0.9) - 1)
    return {
        "label": label,
        "timings_ms": timings,
        "best_ms": min(timings),
        "median_ms": statistics.median(timings),
        "p90_ms": values_sorted[p90_idx],
        "output_sha256": (out_dir / f"{label}-output.sha256").read_text().strip(),
    }

baseline = load_case("baseline")
jeroen = load_case("jeroen")

summary = {
    "schema_version": "1.0",
    "input_file": input_file,
    "runs": runs,
    "warmups": warmups,
    "workers": workers,
    "baseline": baseline,
    "jeroen": jeroen,
    "delta_vs_baseline_ms": jeroen["median_ms"] - baseline["median_ms"],
    "delta_vs_baseline_percent": ((jeroen["median_ms"] - baseline["median_ms"]) / baseline["median_ms"]) * 100.0,
    "speedup_vs_baseline": baseline["median_ms"] / jeroen["median_ms"],
}

(out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY

echo "Comparison artifacts: $out_dir"
