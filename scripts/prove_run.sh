#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-measurements.txt}"
WORKERS="${WORKERS:-$(nproc)}"
TIME_TO_BEAT_MS="${TIME_TO_BEAT_MS:-1535}"
MAIN_CLASS="dev.morling.onebrc.CalculateAverage_jeroen"
OUT_DIR="proofs"
JAR="target/challenge-entry-0.1.0-SNAPSHOT.jar"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

./mvnw -q -DskipTests package

ts="$(date -u +%Y%m%dT%H%M%SZ)"
manifest_file="$OUT_DIR/run-manifest-$ts.json"
output_file="$OUT_DIR/run-output-$ts.txt"
stdout_file="$OUT_DIR/run-stdout-$ts.log"
stderr_file="$OUT_DIR/run-stderr-$ts.log"

read -r -a JAVA_OPTS_ARR <<< "${JAVA_OPTS:-}"
cmd=(env PRINT_PHASE_TIMINGS=1 java "${JAVA_OPTS_ARR[@]}" -cp "$JAR" "$MAIN_CLASS" "$INPUT_FILE" "$WORKERS")
cmd_string="${cmd[*]}"

start_ns="$(date +%s%N)"
set +e
"${cmd[@]}" > "$output_file" 2> "$stderr_file"
exit_code=$?
set -e
end_ns="$(date +%s%N)"
elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
printf "command=%s\nexit_code=%s\nelapsed_ms=%s\n" "$cmd_string" "$exit_code" "$elapsed_ms" > "$stdout_file"
delta_ms=$(( elapsed_ms - TIME_TO_BEAT_MS ))
delta_pct="$(python3 - <<'PY' "$elapsed_ms" "$TIME_TO_BEAT_MS"
import sys
elapsed = float(sys.argv[1])
beat = float(sys.argv[2])
print(f"{((elapsed - beat) / beat) * 100.0:.3f}")
PY
)"

input_sha="$(sha256sum "$INPUT_FILE" | awk '{print $1}')"
jar_sha="$(sha256sum "$JAR" | awk '{print $1}')"
output_sha="$(sha256sum "$output_file" | awk '{print $1}')"
stdout_sha="$(sha256sum "$stdout_file" | awk '{print $1}')"
stderr_sha="$(sha256sum "$stderr_file" | awk '{print $1}')"

split_ms="$(grep -m1 '^phase.split_ms=' "$stderr_file" | cut -d= -f2 || true)"
process_merge_ms="$(grep -m1 '^phase.process_merge_ms=' "$stderr_file" | cut -d= -f2 || true)"
format_ms="$(grep -m1 '^phase.format_ms=' "$stderr_file" | cut -d= -f2 || true)"
total_phase_ms="$(grep -m1 '^phase.total_ms=' "$stderr_file" | cut -d= -f2 || true)"
split_ms="${split_ms:-0}"
process_merge_ms="${process_merge_ms:-0}"
format_ms="${format_ms:-0}"
total_phase_ms="${total_phase_ms:-0}"

git_commit="$(git rev-parse --verify HEAD 2>/dev/null || echo 'UNCOMMITTED')"
git_dirty=false
if [[ -n "$(git status --porcelain 2>/dev/null || true)" ]]; then
  git_dirty=true
fi

prev_manifest_file=""
prev_manifest_sha=""
latest_manifest="$(ls -1 "$OUT_DIR"/run-manifest-*.json 2>/dev/null | sort | tail -n 1 || true)"
if [[ -n "$latest_manifest" ]]; then
  prev_manifest_file="$(basename "$latest_manifest")"
  prev_manifest_sha="$(sha256sum "$latest_manifest" | awk '{print $1}')"
fi

cat > "$manifest_file" <<JSON
{
  "schema_version": "1.0",
  "timestamp_utc": "$ts",
  "input_file": "$INPUT_FILE",
  "input_sha256": "$input_sha",
  "jar_file": "$JAR",
  "jar_sha256": "$jar_sha",
  "output_file": "$(basename "$output_file")",
  "output_sha256": "$output_sha",
  "stdout_file": "$(basename "$stdout_file")",
  "stdout_sha256": "$stdout_sha",
  "stderr_file": "$(basename "$stderr_file")",
  "stderr_sha256": "$stderr_sha",
  "command": "$cmd_string",
  "exit_code": $exit_code,
  "elapsed_ms": $elapsed_ms,
  "phase_split_ms": $split_ms,
  "phase_process_merge_ms": $process_merge_ms,
  "phase_format_ms": $format_ms,
  "phase_total_ms": $total_phase_ms,
  "time_to_beat_ms": $TIME_TO_BEAT_MS,
  "delta_vs_time_to_beat_ms": $delta_ms,
  "delta_vs_time_to_beat_percent": $delta_pct,
  "workers": $WORKERS,
  "java_version": "$(java -version 2>&1 | tr '\n' ' ' | sed 's/"/\\"/g')",
  "git_commit": "$git_commit",
  "git_dirty": $git_dirty,
  "host": {
    "kernel": "$(uname -srmo | sed 's/"/\\"/g')",
    "cpu_model": "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//' | sed 's/"/\\"/g')",
    "core_count": $(nproc)
  },
  "prev_manifest_file": "$prev_manifest_file",
  "prev_manifest_sha256": "$prev_manifest_sha"
}
JSON

echo "Manifest written to: $manifest_file"
./scripts/verify_manifest.sh "$manifest_file"
