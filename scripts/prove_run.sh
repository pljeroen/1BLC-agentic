#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-measurements.txt}"
MAIN_CLASS="dev.morling.onebrc.CalculateAverage_jeroen"
OUT_DIR="proofs"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

epoch="$(date -u +%s)"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
proof_file="$OUT_DIR/run-proof-$ts.json"
output_file="$OUT_DIR/run-output-$ts.txt"

./mvnw -q -DskipTests package

java -cp target/challenge-entry-0.1.0-SNAPSHOT.jar "$MAIN_CLASS" "$INPUT_FILE" > "$output_file"

input_sha="$(sha256sum "$INPUT_FILE" | awk '{print $1}')"
jar_sha="$(sha256sum target/challenge-entry-0.1.0-SNAPSHOT.jar | awk '{print $1}')"
output_sha="$(sha256sum "$output_file" | awk '{print $1}')"

cat > "$proof_file" <<JSON
{
  "timestamp_utc": "$ts",
  "epoch_utc": $epoch,
  "input_file": "$INPUT_FILE",
  "input_sha256": "$input_sha",
  "jar_sha256": "$jar_sha",
  "output_file": "$output_file",
  "output_sha256": "$output_sha",
  "java_version": "$(java -version 2>&1 | tr '\n' ' ' | sed 's/"/\\"/g')",
  "git_commit": "$(git rev-parse --verify HEAD 2>/dev/null || echo 'UNCOMMITTED')",
  "git_dirty": "$(git status --porcelain >/dev/null 2>&1 && [[ -n "$(git status --porcelain)" ]] && echo true || echo false)",
  "command": "java -cp target/challenge-entry-0.1.0-SNAPSHOT.jar $MAIN_CLASS $INPUT_FILE"
}
JSON

echo "Proof written to: $proof_file"
echo "Output written to: $output_file"
