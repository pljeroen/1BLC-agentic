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

echo "[M0/M1] clean test"
./mvnw -q clean test

echo "[M1] verify output against reference"
./scripts/verify_output.sh "$INPUT_FILE"

echo "[M4] worker sweep"
RUNS="$RUNS" WARMUPS="$WARMUPS" MAX_WORKERS="$MAX_WORKERS" TIME_TO_BEAT_MS="$TIME_TO_BEAT_MS" \
  ./scripts/benchmark_workers.sh "$INPUT_FILE"
latest_sweep="$(ls -1 proofs/worker-sweep-*.json | sort | tail -n1)"
recommended_workers="$(python3 - <<'PY' "$latest_sweep"
import json,sys
print(json.load(open(sys.argv[1]))['recommended_workers'])
PY
)"

echo "Recommended workers: $recommended_workers"

echo "[M4] benchmark with recommended workers"
RUNS="$RUNS" WARMUPS="$WARMUPS" WORKERS="$recommended_workers" TIME_TO_BEAT_MS="$TIME_TO_BEAT_MS" \
  ./scripts/benchmark.sh "$INPUT_FILE"
latest_bench="$(ls -1 proofs/bench-*/summary.json | sort | tail -n1)"

echo "[M5] proof run with phase timings + hash chain"
WORKERS="$recommended_workers" TIME_TO_BEAT_MS="$TIME_TO_BEAT_MS" ./scripts/prove_run.sh "$INPUT_FILE"
latest_manifest="$(ls -1 proofs/run-manifest-*.json | sort | tail -n1)"

echo "[M5] replay manifest"
./scripts/replay_manifest.sh "$latest_manifest"

echo "[M6] package record attempt"
./scripts/package_record_attempt.sh "$latest_bench" "$latest_manifest"
latest_package="$(ls -1d proofs/record-attempt-* | sort | tail -n1)"

report="proofs/contract-e2e-$(date -u +%Y%m%dT%H%M%SZ).json"
python3 - <<'PY' "$report" "$INPUT_FILE" "$latest_sweep" "$latest_bench" "$latest_manifest" "$latest_package" "$recommended_workers" "$TIME_TO_BEAT_MS"
import json
import sys

out, input_file, sweep, bench, manifest, package, workers, ttb = sys.argv[1:9]
bench_json = json.load(open(bench))
man_json = json.load(open(manifest))
report = {
  'schema_version': '1.0',
  'contract_status': 'COMPLETE_LOCAL',
  'input_file': input_file,
  'time_to_beat_ms': float(ttb),
  'recommended_workers': int(workers),
  'artifacts': {
    'worker_sweep': sweep,
    'benchmark_summary': bench,
    'run_manifest': manifest,
    'record_package': package,
  },
  'metrics': {
    'best_ms': bench_json['best_ms'],
    'median_ms': bench_json['median_ms'],
    'p90_ms': bench_json['p90_ms'],
    'delta_best_ms': bench_json['delta_best_ms'],
    'delta_median_ms': bench_json['delta_median_ms'],
    'manifest_elapsed_ms': man_json['elapsed_ms'],
    'phase_split_ms': man_json.get('phase_split_ms', 0),
    'phase_process_merge_ms': man_json.get('phase_process_merge_ms', 0),
    'phase_format_ms': man_json.get('phase_format_ms', 0),
    'phase_total_ms': man_json.get('phase_total_ms', 0),
  },
  'notes': [
    'This is a local contract run. Official leaderboard comparability requires 1B-row dataset and official-like hardware/protocol.'
  ]
}
with open(out, 'w') as f:
    json.dump(report, f, indent=2)
    f.write('\n')
print(json.dumps(report, indent=2))
PY

echo "Contract report: $report"
