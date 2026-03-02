# 1BRC Unofficial Entry (Java)

[![Java 25](https://img.shields.io/badge/Java-25-orange)](https://openjdk.org/projects/jdk/25/) [![Build](https://img.shields.io/badge/build-maven-blue)](https://maven.apache.org/) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE) [![Tests](https://img.shields.io/badge/tests-6_passing-green)]() [![Architecture](https://img.shields.io/badge/arch-Unsafe%2BSWAR%2Bbranchless-purple)]() [![Platform](https://img.shields.io/badge/platform-linux--x86__64-blue)]()

A high-performance Java entry for the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc) — computing min/mean/max temperatures for weather stations from a 1-billion-row flat file.

## Results

### Head-to-head comparison

All 16 entries benchmarked on the same hardware: Intel i7-12700H (6P+8E, 20 threads), 64 GB DDR5, NVMe SSD, Linux 6.18, OpenJDK 25, EpsilonGC. 413 stations, 1B rows, 13.8 GB. Each entry: 5 timed runs, 2 warmups, 60s cooldown between entries. Deterministic output verified (SHA `426c10e8...`).

| # | Entry | Best | Median | p90 | Official | Notes |
|---|---|---|---|---|---|---|
| 1 | jerrinot | 1,322 ms | 1,377 ms | 1,398 ms | 1,608 ms | GraalVM native, Unsafe |
| 2 | serkan_ozal | 1,320 ms | 1,399 ms | 1,421 ms | 1,880 ms | Vector API, Unsafe |
| 3 | thomaswue | 1,451 ms | 1,504 ms | 1,550 ms | 1,535 ms | winner, Unsafe |
| 4 | stephenvonworley | 1,462 ms | 1,527 ms | 1,586 ms | 2,018 ms | GraalVM native, Unsafe |
| 5 | artsiomkorzun | 1,478 ms | 1,536 ms | 1,580 ms | 1,587 ms | GraalVM native, Unsafe |
| 6 | merykittyunsafe | 1,436 ms | 1,540 ms | 1,551 ms | 2,367 ms | Vector API, Unsafe |
| 7 | abeobk | 1,492 ms | 1,557 ms | 1,595 ms | 1,921 ms | GraalVM native, Unsafe |
| **8** | **Claude + Jeroen** | **1,660 ms** | **1,677 ms** | **1,752 ms** | — | **Unsafe, Arena** |
| 9 | tivrfoa | 1,609 ms | 1,679 ms | 1,688 ms | 2,995 ms | GraalVM native, Unsafe |
| 10 | yavuztas | 1,590 ms | 1,701 ms | 1,759 ms | 2,319 ms | GraalVM native, Unsafe |
| 11 | gonix | 1,663 ms | 1,709 ms | 1,728 ms | 2,997 ms | standard Java |
| 12 | merykitty | 1,713 ms | 1,741 ms | 1,749 ms | 3,210 ms | Vector API |
| 13 | royvanrijn | 1,693 ms | 1,744 ms | 1,777 ms | 2,157 ms | GraalVM native, Unsafe |
| 14 | yourwass | 1,662 ms | 1,759 ms | 1,764 ms | 2,557 ms | Vector API, Unsafe |
| 15 | JamalMulla | 1,752 ms | 1,770 ms | 1,784 ms | 3,095 ms | GraalVM native, Unsafe |
| 16 | mtopolnik | 1,699 ms | 1,780 ms | 1,868 ms | 2,332 ms | GraalVM native, Unsafe |
| 17 | roman_r_m | 1,905 ms | 1,953 ms | 2,027 ms | 3,431 ms | GraalVM native, Unsafe |

The "Official" column shows times from the [original leaderboard](https://github.com/gunnarmorling/1brc) (32-core AMD EPYC 7502P). Rankings differ from the official board because hardware-specific optimizations (Vector API, GraalVM native image) scale differently on consumer Intel vs server AMD.

Full per-entry benchmark data: [`docs/bench-comparison-20260302/`](docs/bench-comparison-20260302/).

### Standalone results

| Dataset | Stations | File size | Rows | Best | Median | p90 |
|---|---|---|---|---|---|---|
| **Official** (413 stations) | 413 | 13.8 GB | 1,000,000,000 | **1.66 s** | 1.68 s | 1.75 s |

### Cache pressure: 413 vs 10K stations

- **413 stations**: hash table working set is ~26 KB, fits entirely in L1 cache (48 KB). Every lookup is ~4 cycles.
- **10K stations**: working set grows to ~640 KB, spills to L2/L3. Lookups cost 30-100 cycles. Station names are also much longer (up to 100 characters), increasing parsing and comparison overhead.

## Approach

Single-file, zero-dependency Java solution using:

- **Memory-mapped I/O** via `Arena.global()` — single contiguous mmap of the entire file, accessed through `sun.misc.Unsafe`
- **SWAR semicolon detection** — Mycroft's `hasByte` trick, scanning 8 bytes per cycle
- **Branchless temperature parser** — dot-position detection via bit manipulation, single multiply for decimal conversion
- **XOR-accumulate hash** — simple XOR folding during name scan, single golden-ratio finalizer multiply
- **Off-heap cache-line-aligned hash table** — each slot is exactly 64 bytes (1 cache line), allocated off-heap with guaranteed 64-byte alignment via `Unsafe.allocateMemory`. Packs hash, stats, and 32 bytes of inline name data. Occupied-slot list for O(n) merge and iteration
- **Work-stealing parallelism** — atomic chunk counter, ~4 MB chunks aligned to newline boundaries, scales across heterogeneous P-core/E-core topologies
- **JIT-optimized** — `-XX:-TieredCompilation` skips C1, compiling directly to C2; `-XX:-UseCountedLoopSafepoints` eliminates safepoint polling from hot loops

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
src/test/java/.../CalculateAverageJeroenTest.java 6 tests (parser, determinism, large-dataset)
scripts/benchmark.sh                              Benchmark runner with statistical summary
scripts/benchmark_all.sh                          Head-to-head comparison runner
scripts/verify_output.sh                          Output verification against reference impl
docs/bench-413-stations.json                      Standalone benchmark proof
docs/bench-10k-stations.json                      Extended benchmark proof (10K stations)
docs/bench-comparison-20260302/                   Head-to-head vs top 16 official entries
```

## Acknowledgments

This entry builds on techniques pioneered by other 1BRC participants:

- **Branchless temperature parser** — [Quan Anh Mai (merykitty)](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java): dot-position detection via `~word & 0x10101000`, sign-extension trick, and the `0x640a0001` magic multiply for single-instruction decimal conversion
- **SWAR semicolon detection** — [Alan Mycroft](https://en.wikipedia.org/wiki/Alan_Mycroft)'s `hasByte` technique for finding a byte in a 64-bit word using `(xor - 0x0101...01) & ~xor & 0x8080...80`
- **Cache-line-sized AoS hash table** — inspired by [Thomas Wuerthinger (thomaswue)](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java) and [gonix](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonix.java): packing hash, stats, and inline name data into a single 64-byte cache line per slot
- **Work-stealing chunk parallelism** — atomic counter pattern used across many top entries (thomaswue, artsiomkorzun, and others)
- **JVM tuning** — EpsilonGC, `-XX:-TieredCompilation`, `-XX:-UseCountedLoopSafepoints` — collectively discovered and shared by the 1BRC community

Written by [Claude](https://claude.ai/) (Anthropic), orchestrated by Jeroen.

## License

Apache-2.0 ([`LICENSE`](LICENSE)).
