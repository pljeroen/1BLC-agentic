# 1BRC Challenge Entry (Java)
[![Java 21](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Build](https://img.shields.io/badge/build-maven-blue)](https://maven.apache.org/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

This repository is a clean, standalone Java project for building a 1BRC entry from scratch.

Execution contract:
- `docs/record-attempt-contract.md` - milestones `M0..M6` with measurable gates and provenance requirements

## Layout

- `src/main/java/dev/morling/onebrc/CalculateAverage_jeroen.java`: challenge entry point
- `scripts/prove_run.sh`: generates a cryptographic run proof manifest with hash chain + time-to-beat deltas
- `scripts/verify_output.sh`: verifies output against reference implementation
- `scripts/benchmark.sh`: repeatable benchmark runner with best/median/p90 and time-to-beat deltas
- `scripts/benchmark_workers.sh`: adaptive worker-count sweep with median-based recommendation
- `scripts/verify_manifest.sh`: validates manifest integrity and artifact hashes
- `scripts/replay_manifest.sh`: reruns a manifest command and verifies replay output hash
- `scripts/package_record_attempt.sh`: assembles benchmark + manifest into a record-attempt package
- `scripts/run_contract_e2e.sh`: executes contract gates end-to-end and emits a final contract report
- `docs/run-manifest.schema.json`: JSON schema for run proof manifests
- `proofs/`: proof artifacts

## Quick start

```bash
./mvnw test
./mvnw -DskipTests package
java -cp target/challenge-entry-0.1.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_jeroen measurements.txt
```

## Run proof

```bash
./scripts/prove_run.sh measurements.txt
```

This emits:
- `proofs/run-output-<timestamp>.txt`
- `proofs/run-manifest-<timestamp>.json`
- `proofs/run-stdout-<timestamp>.log`
- `proofs/run-stderr-<timestamp>.log`

## License

Apache-2.0 (`LICENSE`).
