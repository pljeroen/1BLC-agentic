#!/usr/bin/env bash
set -euo pipefail

# Benchmark all contenders sequentially with cooldowns between runs.
# Usage: scripts/benchmark_all.sh [cooldown_seconds] [runs] [warmups]

COOLDOWN="${1:-60}"
RUNS="${2:-5}"
WARMUPS="${3:-2}"
RESULTS_DIR="proofs/comparison-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RESULTS_DIR"

JAVA_OPTS="--add-opens java.base/sun.misc=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xms4g -Xmx4g"
JAVA_OPTS_VECTOR="$JAVA_OPTS --add-modules jdk.incubator.vector"

# Entries: name, classpath, extra_java_opts, pass_input, pass_workers
declare -a ENTRIES=(
  "thomaswue|target/thomaswue||0|0"
  "artsiomkorzun|target/contenders/artsiomkorzun||0|0"
  "jerrinot|target/contenders/jerrinot||0|0"
  "serkan_ozal|target/contenders/serkan_ozal|--add-modules jdk.incubator.vector|0|0"
  "abeobk|target/contenders/abeobk||0|0"
  "stephenvonworley|target/contenders/stephenvonworley||0|0"
  "royvanrijn|target/contenders/royvanrijn||0|0"
  "yavuztas|target/contenders/yavuztas||0|0"
  "mtopolnik|target/contenders/mtopolnik||0|0"
  "merykittyunsafe|target/contenders/merykittyunsafe|--add-modules jdk.incubator.vector|0|0"
  "yourwass|target/contenders/yourwass|--add-modules jdk.incubator.vector|0|0"
  "tivrfoa|target/contenders/tivrfoa||0|0"
  "gonix|target/contenders/gonix||0|0"
  "JamalMulla|target/contenders/JamalMulla||0|0"
  "merykitty|target/contenders/merykitty|--add-modules jdk.incubator.vector|0|0"
  "roman_r_m|target/contenders/roman_r_m||0|0"
  "jeroen|target/challenge-entry-0.1.0-SNAPSHOT.jar||1|1"
)

total=${#ENTRIES[@]}
echo "Benchmarking $total entries: $RUNS runs, $WARMUPS warmups, ${COOLDOWN}s cooldown"
echo "Results: $RESULTS_DIR"
echo ""

declare -a SUMMARY_LINES=()
idx=0

for entry in "${ENTRIES[@]}"; do
  IFS='|' read -r name classpath extra_opts pass_input pass_workers <<< "$entry"
  idx=$((idx + 1))

  echo "[$idx/$total] Cooling ${COOLDOWN}s before $name..."
  sleep "$COOLDOWN"

  echo "[$idx/$total] Running $name..."

  entry_opts="$JAVA_OPTS"
  if [[ -n "$extra_opts" ]]; then
    entry_opts="$entry_opts $extra_opts"
  fi

  out_file="$RESULTS_DIR/${name}.json"

  if MAIN_CLASS="dev.morling.onebrc.CalculateAverage_${name}" \
     CLASS_PATH="$classpath" \
     JAVA_OPTS="$entry_opts" \
     PASS_INPUT_ARG="$pass_input" \
     PASS_WORKERS_ARG="$pass_workers" \
     RUNS="$RUNS" \
     WARMUPS="$WARMUPS" \
     scripts/benchmark.sh measurements.txt 2>/dev/null | grep -v '^Benchmark artifacts:' > "$out_file"; then

    best=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d['best_ms'])" "$out_file")
    median=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d['median_ms'])" "$out_file")
    p90=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d['p90_ms'])" "$out_file")
    SUMMARY_LINES+=("$(printf '%7s %7s %7s  %s' "$best" "$median" "$p90" "$name")")
    echo "  -> best=${best}ms  median=${median}ms  p90=${p90}ms"
  else
    SUMMARY_LINES+=("$(printf '%7s %7s %7s  %s' "FAIL" "FAIL" "FAIL" "$name")")
    echo "  -> FAILED (runtime error)"
  fi
  echo ""
done

echo "======================================"
echo "FINAL RESULTS (sorted by median)"
echo "======================================"
printf '%7s %7s %7s  %s\n' "best" "median" "p90" "entry"
printf '%7s %7s %7s  %s\n' "-------" "-------" "-------" "-----"
printf '%s\n' "${SUMMARY_LINES[@]}" | sort -t' ' -k2 -n
echo ""
echo "Results saved to: $RESULTS_DIR"

# Save sorted summary
{
  printf '%7s %7s %7s  %s\n' "best" "median" "p90" "entry"
  printf '%7s %7s %7s  %s\n' "-------" "-------" "-------" "-----"
  printf '%s\n' "${SUMMARY_LINES[@]}" | sort -t' ' -k2 -n
} > "$RESULTS_DIR/summary.txt"
