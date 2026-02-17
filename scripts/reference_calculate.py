#!/usr/bin/env python3
import math
import sys
from collections import defaultdict


def java_round(x: float) -> int:
    return math.floor(x + 0.5)


def format_tenths(v: int) -> str:
    sign = '-' if v < 0 else ''
    v = abs(v)
    return f"{sign}{v // 10}.{v % 10}"


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else 'measurements.txt'
    stats = defaultdict(lambda: [10_000, -10_000, 0, 0])

    with open(path, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            station, temp = line.split(';', 1)
            value_tenths = int(round(float(temp) * 10))
            s = stats[station]
            if value_tenths < s[0]:
                s[0] = value_tenths
            if value_tenths > s[1]:
                s[1] = value_tenths
            s[2] += value_tenths
            s[3] += 1

    parts = []
    for station in sorted(stats.keys()):
        min_t, max_t, sum_t, cnt = stats[station]
        mean_t = java_round(sum_t / cnt)
        parts.append(f"{station}={format_tenths(min_t)}/{format_tenths(mean_t)}/{format_tenths(max_t)}")

    print('{' + ', '.join(parts) + '}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
