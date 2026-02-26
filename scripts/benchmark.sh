#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-measurements.txt}"
RUNS="${RUNS:-10}"
WARMUPS="${WARMUPS:-2}"
WORKERS="${WORKERS:-$(nproc)}"
TIME_TO_BEAT_MS="${TIME_TO_BEAT_MS:-1535}"
MAIN_CLASS="${MAIN_CLASS:-dev.morling.onebrc.CalculateAverage_jeroen}"
JAR="${JAR:-target/challenge-entry-0.1.0-SNAPSHOT.jar}"
CLASS_PATH="${CLASS_PATH:-$JAR}"
PASS_INPUT_ARG="${PASS_INPUT_ARG:-1}"
PASS_WORKERS_ARG="${PASS_WORKERS_ARG:-1}"

# Default JVM opts for Unsafe access + EpsilonGC
if [[ -z "${JAVA_OPTS:-}" ]]; then
  JAVA_OPTS="--add-opens java.base/sun.misc=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xms4g -Xmx4g"
fi

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

if [[ "$CLASS_PATH" == "$JAR" && ! -f "$JAR" ]]; then
  ./mvnw -q -DskipTests package
fi

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="proofs/bench-$ts"
mkdir -p "$out_dir"

read -r -a JAVA_OPTS_ARR <<< "${JAVA_OPTS:-}"
cmd=(java "${JAVA_OPTS_ARR[@]}" -cp "$CLASS_PATH" "$MAIN_CLASS")
if [[ "$PASS_INPUT_ARG" == "1" ]]; then
  cmd+=("$INPUT_FILE")
fi
if [[ "$PASS_WORKERS_ARG" == "1" ]]; then
  cmd+=("$WORKERS")
fi

for ((i=1; i<=WARMUPS; i++)); do
  "${cmd[@]}" > "$out_dir/warmup-$i.out"
done

declare -a timings_ms
for ((i=1; i<=RUNS; i++)); do
  start_ns="$(date +%s%N)"
  "${cmd[@]}" > "$out_dir/run-$i.out"
  end_ns="$(date +%s%N)"
  elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
  timings_ms+=("$elapsed_ms")
  printf "%s\n" "$elapsed_ms" > "$out_dir/run-$i.ms"
done

output_ref_sha="$(sha256sum "$out_dir/run-1.out" | awk '{print $1}')"
for ((i=2; i<=RUNS; i++)); do
  sha="$(sha256sum "$out_dir/run-$i.out" | awk '{print $1}')"
  if [[ "$sha" != "$output_ref_sha" ]]; then
    echo "Non-deterministic output detected between benchmark runs" >&2
    exit 2
  fi
done

printf "%s\n" "${timings_ms[@]}" > "$out_dir/timings_ms.txt"

file_size_bytes="$(wc -c < "$INPUT_FILE")"
station_count="$(grep -o '=' "$out_dir/run-1.out" | wc -l)"

python3 - <<'PY' "$out_dir/timings_ms.txt" "$RUNS" "$WARMUPS" "$WORKERS" "$INPUT_FILE" "$output_ref_sha" "$out_dir" "$TIME_TO_BEAT_MS" "$file_size_bytes" "$station_count"
import json
import platform
import statistics
import sys
from pathlib import Path

p = Path(sys.argv[1])
runs = int(sys.argv[2])
warmups = int(sys.argv[3])
workers = int(sys.argv[4])
input_file = sys.argv[5]
output_sha = sys.argv[6]
out_dir = Path(sys.argv[7])
time_to_beat_ms = float(sys.argv[8])
file_size_bytes = int(sys.argv[9])
station_count = int(sys.argv[10])
values = [int(x.strip()) for x in p.read_text().splitlines() if x.strip()]
values_sorted = sorted(values)
median = statistics.median(values)
best = min(values)
p90_idx = max(0, int(len(values_sorted) * 0.9) - 1)
p90 = values_sorted[p90_idx]
summary = {
    "schema_version": "1.0",
    "runs": runs,
    "warmups": warmups,
    "workers": workers,
    "input_file": input_file,
    "timings_ms": values,
    "best_ms": best,
    "median_ms": median,
    "p90_ms": p90,
    "time_to_beat_ms": time_to_beat_ms,
    "delta_best_ms": best - time_to_beat_ms,
    "delta_median_ms": median - time_to_beat_ms,
    "delta_p90_ms": p90 - time_to_beat_ms,
    "delta_best_percent": ((best - time_to_beat_ms) / time_to_beat_ms) * 100.0,
    "delta_median_percent": ((median - time_to_beat_ms) / time_to_beat_ms) * 100.0,
    "delta_p90_percent": ((p90 - time_to_beat_ms) / time_to_beat_ms) * 100.0,
    "file_size_bytes": file_size_bytes,
    "station_count": station_count,
    "deterministic_output_sha256": output_sha,
    "host": {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": platform.python_version(),
    },
}
(out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY

echo "Benchmark artifacts: $out_dir"
