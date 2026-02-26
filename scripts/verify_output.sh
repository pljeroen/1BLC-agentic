#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-measurements.txt}"
WORKERS="${WORKERS:-$(nproc)}"
MAIN_CLASS="dev.morling.onebrc.CalculateAverage_jeroen"
JAR="target/challenge-entry-0.1.0-SNAPSHOT.jar"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

./mvnw -q -DskipTests package

# Default JVM opts for Unsafe access
if [[ -z "${JAVA_OPTS:-}" ]]; then
  JAVA_OPTS="--add-opens java.base/sun.misc=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED"
fi
read -r -a JAVA_OPTS_ARR <<< "$JAVA_OPTS"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

java "${JAVA_OPTS_ARR[@]}" -cp "$JAR" "$MAIN_CLASS" "$INPUT_FILE" "$WORKERS" > "$tmp_dir/actual.txt"
./scripts/reference_calculate.py "$INPUT_FILE" > "$tmp_dir/expected.txt"

if ! diff -u "$tmp_dir/expected.txt" "$tmp_dir/actual.txt" >/dev/null; then
  echo "Output mismatch against reference implementation" >&2
  diff -u "$tmp_dir/expected.txt" "$tmp_dir/actual.txt" || true
  exit 2
fi

actual_sha="$(sha256sum "$tmp_dir/actual.txt" | awk '{print $1}')"
expected_sha="$(sha256sum "$tmp_dir/expected.txt" | awk '{print $1}')"

echo "Verification passed"
echo "expected_sha256=$expected_sha"
echo "actual_sha256=$actual_sha"
