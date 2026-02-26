# 1BRC Challenge Entry (Java)

[![Java 25](https://img.shields.io/badge/Java-25-orange)](https://openjdk.org/projects/jdk/25/) [![Build](https://img.shields.io/badge/build-maven-blue)](https://maven.apache.org/) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE) [![Tests](https://img.shields.io/badge/tests-5_passing-green)]() [![Architecture](https://img.shields.io/badge/arch-Unsafe%2BSWAR%2Bbranchless-purple)]() [![Platform](https://img.shields.io/badge/platform-linux--x86__64-blue)]()

A high-performance Java entry for the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc) — computing min/mean/max temperatures for weather stations from a 1-billion-row flat file.

## Results

Benchmarked on an Intel i7-12700H (6P+8E cores, 20 threads), 64 GB DDR5, NVMe SSD, Linux 6.18, OpenJDK 25.

| Dataset | Stations | File size | Rows | Best | Median | p90 |
|---|---|---|---|---|---|---|
| **Official** (413 stations) | 413 | 13.8 GB | 1,000,000,000 | **1.80 s** | 1.88 s | 2.02 s |
| **Extended** (10K stations) | 10,000 | 17.0 GB | 1,000,000,000 | **5.21 s** | 5.41 s | 5.53 s |

10 timed runs, 2 warmup runs, deterministic output verified via SHA-256 across all runs. Full benchmark data in [`docs/bench-413-stations.json`](docs/bench-413-stations.json) and [`docs/bench-10k-stations.json`](docs/bench-10k-stations.json).

### Why two benchmarks?

The 413-station result shows what the code does on the official challenge dataset. The 10K-station result reveals what happens when the hash table no longer fits in L1 cache:

- **413 stations**: hash table working set is ~26 KB, fits entirely in L1 cache (48 KB). Every lookup is ~4 cycles.
- **10K stations**: working set grows to ~640 KB, spills to L2/L3. Lookups cost 30-100 cycles. Station names are also much longer (up to 100 characters), increasing parsing and comparison overhead.

The 2.9x slowdown (1.80 s vs 5.21 s) for 24x more stations on a 23% larger file demonstrates that the implementation is genuinely compute-bound, not I/O-bound.

## Approach

Single-file, zero-dependency Java solution using:

- **Memory-mapped I/O** via `sun.misc.Unsafe` — 1 GB segments with overlap for line-boundary safety
- **SWAR semicolon detection** — Mycroft's `hasByte` trick, scanning 8 bytes per cycle
- **Branchless temperature parser** — dot-position detection via bit manipulation, single multiply for decimal conversion
- **Cache-line-aligned hash table** — each slot is exactly 64 bytes (1 cache line), packing hash, stats, and 32 bytes of inline name data. Eliminates the 7+ cache line accesses typical of struct-of-arrays layouts
- **Work-stealing parallelism** — atomic chunk counter, ~4 MB chunks aligned to newline boundaries, scales across heterogeneous P-core/E-core topologies

## Quick start

```bash
# Run tests
./mvnw test

# Build and run
./mvnw -DskipTests package
java --add-opens java.base/sun.misc=ALL-UNNAMED \
     --add-opens java.base/java.nio=ALL-UNNAMED \
     -cp target/challenge-entry-0.1.0-SNAPSHOT.jar \
     dev.morling.onebrc.CalculateAverage_jeroen measurements.txt

# Full benchmark (10 runs + 2 warmups)
scripts/benchmark.sh measurements.txt
```

## Project layout

```
src/main/java/.../CalculateAverage_jeroen.java   Challenge entry
src/test/java/.../CalculateAverageJeroenTest.java 5 tests (including exhaustive parser coverage)
scripts/benchmark.sh                              Benchmark runner with statistical summary
scripts/verify_output.sh                          Output verification against reference impl
docs/bench-413-stations.json                      Official benchmark proof
docs/bench-10k-stations.json                      Extended benchmark proof
```

## License

Apache-2.0 ([`LICENSE`](LICENSE)).
