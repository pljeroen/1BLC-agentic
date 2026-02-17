# 1BRC Challenge Entry (Java)

This repository is a clean, standalone Java project for building a 1BRC entry from scratch.

## Layout

- `src/main/java/dev/morling/onebrc/CalculateAverage_jeroen.java`: challenge entry point
- `scripts/prove_run.sh`: generates a cryptographic run proof (SHA-256 of input/jar/output)
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
- `proofs/run-proof-<timestamp>.json`
