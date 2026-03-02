package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class CalculateAverage_jeroen {

    private static final sun.misc.Unsafe UNSAFE;
    private static final long BYTE_ARRAY_BASE;

    static {
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
            BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final int CHUNK_SIZE = 1 << 22; // 4 MB
    private static final int TABLE_SIZE = 1 << 14; // 16384
    private static final int TABLE_MASK = TABLE_SIZE - 1;
    private static final int SAFE_MARGIN = 128;
    private static final long SEMICOLON_PATTERN = 0x3B3B3B3B3B3B3B3BL;

    private CalculateAverage_jeroen() {
    }

    // ---- Public API ----

    public static void main(String[] args) throws Exception {
        String file = args.length > 0 ? args[0] : "measurements.txt";
        int workers = args.length > 1
                ? Integer.parseInt(args[1])
                : Math.max(1, Runtime.getRuntime().availableProcessors());
        System.out.println(format(calculate(Path.of(file), workers)));
        System.out.close();
    }

    public static Map<String, Stats> calculate(Path file, int workers)
            throws IOException, InterruptedException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize == 0) {
                return Map.of();
            }

            long fileStart = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize,
                    Arena.global()).address();
            long fileEnd = fileStart + fileSize;

            int maxChunks = (int) (fileSize / CHUNK_SIZE) + 2;
            long[] chunkStart = new long[maxChunks];
            long[] chunkEnd = new long[maxChunks];
            int numChunks = 0;

            long pos = fileStart;
            while (pos < fileEnd) {
                long end = Math.min(pos + CHUNK_SIZE, fileEnd);
                if (end < fileEnd) {
                    while (end < fileEnd && UNSAFE.getByte(end) != '\n') {
                        end++;
                    }
                    if (end < fileEnd) {
                        end++;
                    }
                }
                chunkStart[numChunks] = pos;
                chunkEnd[numChunks] = end;
                numChunks++;
                pos = end;
            }

            int safeWorkers = Math.max(1, Math.min(workers, numChunks));
            AtomicInteger counter = new AtomicInteger(0);
            StationTable[] tables = new StationTable[safeWorkers];
            Thread[] threads = new Thread[safeWorkers];

            final int finalNumChunks = numChunks;
            for (int t = 0; t < safeWorkers; t++) {
                final int ti = t;
                threads[t] = new Thread(() -> {
                    StationTable table = new StationTable();
                    tables[ti] = table;
                    int ci;
                    while ((ci = counter.getAndIncrement()) < finalNumChunks) {
                        processChunk(chunkStart[ci], chunkEnd[ci], table);
                    }
                });
                threads[t].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            StationTable merged = tables[0];
            for (int t = 1; t < safeWorkers; t++) {
                if (tables[t] != null) {
                    merged.mergeFrom(tables[t]);
                }
            }

            Map<String, Stats> result = new HashMap<>(1024);
            merged.forEach((name, nameLen, minVal, maxVal, sumVal, countVal) -> {
                result.put(new String(name, 0, nameLen, StandardCharsets.UTF_8),
                        Stats.fromAggregate(minVal, maxVal, sumVal, countVal));
            });

            return result;
        }
    }

    public static String format(Map<String, Stats> statsByStation) {
        TreeMap<String, Stats> sorted = new TreeMap<>(statsByStation);
        StringBuilder out = new StringBuilder(sorted.size() * 32);
        out.append('{');
        boolean first = true;
        for (Map.Entry<String, Stats> e : sorted.entrySet()) {
            if (!first) {
                out.append(", ");
            }
            first = false;
            Stats s = e.getValue();
            out.append(e.getKey()).append('=')
                    .append(formatTenths(s.minTenths)).append('/')
                    .append(formatTenths(roundedMeanTenths(s.sumTenths, s.count))).append('/')
                    .append(formatTenths(s.maxTenths));
        }
        out.append('}');
        return out.toString();
    }

    static long roundedMeanTenths(long sumTenths, long count) {
        return Math.round(sumTenths / (double) count);
    }

    static int parseTemperatureTenths(byte[] line, int start, int endExclusive) {
        int i = start;
        int sign = 1;
        if (line[i] == '-') {
            sign = -1;
            i++;
        }
        int whole = 0;
        while (i < endExclusive && line[i] != '.') {
            whole = whole * 10 + (line[i] - '0');
            i++;
        }
        i++;
        int frac = i < endExclusive ? line[i] - '0' : 0;
        return sign * (whole * 10 + frac);
    }

    // ---- Inner loop (SWAR + branchless) ----

    private static void processChunk(long addr, long endAddr, StationTable table) {
        long safeEnd = endAddr - SAFE_MARGIN;

        while (addr < safeEnd) {
            long nameAddr = addr;
            long hash = 0;
            long word;
            long semi;

            while (true) {
                word = UNSAFE.getLong(addr);
                semi = hasByte(word, SEMICOLON_PATTERN);
                if (semi != 0) {
                    break;
                }
                hash = mixHash(hash, word);
                addr += 8;
            }

            int semiPos = Long.numberOfTrailingZeros(semi) >>> 3;
            hash = mixHash(hash, word & ((1L << (semiPos << 3)) - 1));

            long semiAddr = addr + semiPos;
            int nameLen = (int) (semiAddr - nameAddr);
            int finalHash = finalMix(hash);

            long tempWord = UNSAFE.getLong(semiAddr + 1);
            int dotBitPos = Long.numberOfTrailingZeros(~tempWord & 0x10101000); // merykitty
            int temp = parseTemperatureBranchless(tempWord, dotBitPos);
            addr = semiAddr + 1 + (dotBitPos >>> 3) + 3;

            table.accumulate(nameAddr, nameLen, finalHash, temp);
        }

        if (addr < endAddr) {
            processChunkSafe(addr, endAddr, table);
        }
    }

    private static void processChunkSafe(long addr, long endAddr, StationTable table) {
        long lineStart = addr;
        while (addr < endAddr) {
            if (UNSAFE.getByte(addr) == '\n') {
                if (addr > lineStart) {
                    processLineSafe(lineStart, addr, table);
                }
                lineStart = addr + 1;
            }
            addr++;
        }
        if (endAddr > lineStart) {
            processLineSafe(lineStart, endAddr, table);
        }
    }

    private static void processLineSafe(long start, long end, StationTable table) {
        while (end > start && UNSAFE.getByte(end - 1) == '\r') {
            end--;
        }
        if (end - start <= 2) {
            return;
        }

        long addr = start;
        long hash = 0;
        long word = 0;
        int byteInWord = 0;

        while (addr < end && UNSAFE.getByte(addr) != ';') {
            word |= ((long) (UNSAFE.getByte(addr) & 0xFF)) << (byteInWord << 3);
            byteInWord++;
            if (byteInWord == 8) {
                hash = mixHash(hash, word);
                word = 0;
                byteInWord = 0;
            }
            addr++;
        }
        if (addr >= end) {
            return;
        }
        if (byteInWord > 0) {
            hash = mixHash(hash, word);
        }
        int finalHash = finalMix(hash);

        int nameLen = (int) (addr - start);

        addr++;
        int sign = 1;
        if (addr < end && UNSAFE.getByte(addr) == '-') {
            sign = -1;
            addr++;
        }
        int whole = 0;
        while (addr < end && UNSAFE.getByte(addr) != '.') {
            whole = whole * 10 + (UNSAFE.getByte(addr) - '0');
            addr++;
        }
        addr++;
        int frac = (addr < end) ? (UNSAFE.getByte(addr) - '0') : 0;
        int temp = sign * (whole * 10 + frac);

        table.accumulate(start, nameLen, finalHash, temp);
    }

    // ---- SWAR helpers ----

    // Alan Mycroft's hasByte trick: detects a target byte in a 64-bit word in O(1).
    private static long hasByte(long word, long pattern) {
        long xor = word ^ pattern;
        return (xor - 0x0101010101010101L) & ~xor & 0x8080808080808080L;
    }

    private static long mixHash(long hash, long word) {
        return hash ^ word;
    }

    private static int finalMix(long h) {
        h *= 0x9E3779B97F4A7C15L;
        return (int) (h >>> 17);
    }

    // Branchless temperature parser by Quan Anh Mai (merykitty).
    // Dot-position detection, sign-extension, and 0x640a0001 magic multiply
    // for single-instruction decimal conversion.
    static int parseTemperatureBranchless(long word, int dotBitPos) {
        long inv = ~word;
        long signed = (inv << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        long digits = ((word & designMask) << (28 - dotBitPos)) & 0x0F000F0F00L;
        long absValue = ((digits * 0x640a0001L) >>> 32) & 0x3FF;
        return (int) ((absValue ^ signed) - signed);
    }

    // ---- Formatting ----

    private static String formatTenths(long tenths) {
        long abs = Math.abs(tenths);
        return (tenths < 0 ? "-" : "") + (abs / 10) + "." + (abs % 10);
    }

    // ---- Stats ----

    public static final class Stats {
        int minTenths;
        int maxTenths;
        long sumTenths;
        long count;

        Stats(int valueTenths) {
            this.minTenths = valueTenths;
            this.maxTenths = valueTenths;
            this.sumTenths = valueTenths;
            this.count = 1;
        }

        static Stats fromAggregate(int min, int max, long sum, long count) {
            Stats s = new Stats(min);
            s.maxTenths = max;
            s.sumTenths = sum;
            s.count = count;
            return s;
        }

        void mergeIn(int min, int max, long sum, long cnt) {
            if (min < minTenths) {
                minTenths = min;
            }
            if (max > maxTenths) {
                maxTenths = max;
            }
            sumTenths += sum;
            count += cnt;
        }
    }

    // ---- Hash Table ----

    private interface EntryConsumer {
        void accept(byte[] name, int nameLen, int min, int max, long sum, long count);
    }

    // AoS hash table design inspired by thomaswue and gonix: one cache line per slot.
    // Off-heap allocation with 64-byte alignment to avoid cross-cache-line splits.
    private static final class StationTable {
        // AoS layout: 1 cache line (64 bytes) per slot, off-heap, 64-byte aligned.
        // [0:8]   packed: (hash<<32)|nameLen, 0=empty
        // [8:16]  sum (long)
        // [16:24] count (long)
        // [24:28] min (int)
        // [28:32] max (int)
        // [32:64] inline name (first 32 bytes)
        private static final int ENTRY_BYTES = 64;
        private static final int E_PACKED = 0;
        private static final int E_SUM = 8;
        private static final int E_COUNT = 16;
        private static final int E_MIN = 24;
        private static final int E_MAX = 28;
        private static final int E_NAME = 32;
        private static final int INLINE_MAX = 32;

        private final long dataAddr;
        private final byte[][] keys = new byte[TABLE_SIZE][];
        private final int[] occupied = new int[1024];
        private int occupiedCount = 0;

        StationTable() {
            long raw = UNSAFE.allocateMemory((long) TABLE_SIZE * ENTRY_BYTES + 64);
            dataAddr = (raw + 63) & ~63L;
            UNSAFE.setMemory(dataAddr, (long) TABLE_SIZE * ENTRY_BYTES, (byte) 0);
        }

        void accumulate(long nameAddr, int nameLen, int hash, int temp) {
            long firstWord = (nameLen >= 8)
                    ? UNSAFE.getLong(nameAddr)
                    : readPartialWord(nameAddr, nameLen);
            long packedHashLen = ((long) hash << 32) | (nameLen & 0xFFFFFFFFL);
            int idx = hash & TABLE_MASK;

            while (true) {
                long base = dataAddr + (long) idx * ENTRY_BYTES;
                long stored = UNSAFE.getLong(base + E_PACKED);

                if (stored == packedHashLen
                        && UNSAFE.getLong(base + E_NAME) == firstWord
                        && matchNameRest(base, nameAddr, nameLen, idx)) {
                    long curSum = UNSAFE.getLong(base + E_SUM);
                    long curCount = UNSAFE.getLong(base + E_COUNT);
                    int curMin = UNSAFE.getInt(base + E_MIN);
                    int curMax = UNSAFE.getInt(base + E_MAX);
                    UNSAFE.putLong(base + E_SUM, curSum + temp);
                    UNSAFE.putLong(base + E_COUNT, curCount + 1);
                    if (temp < curMin) {
                        UNSAFE.putInt(base + E_MIN, temp);
                    }
                    if (temp > curMax) {
                        UNSAFE.putInt(base + E_MAX, temp);
                    }
                    return;
                }

                if (stored == 0) {
                    byte[] name = new byte[nameLen];
                    UNSAFE.copyMemory(null, nameAddr, name, BYTE_ARRAY_BASE, nameLen);
                    keys[idx] = name;
                    occupied[occupiedCount++] = idx;
                    UNSAFE.putLong(base + E_PACKED, packedHashLen);
                    UNSAFE.putLong(base + E_SUM, temp);
                    UNSAFE.putLong(base + E_COUNT, 1);
                    UNSAFE.putInt(base + E_MIN, temp);
                    UNSAFE.putInt(base + E_MAX, temp);
                    int copyLen = Math.min(nameLen, INLINE_MAX);
                    UNSAFE.copyMemory(nameAddr, base + E_NAME, copyLen);
                    return;
                }

                idx = (idx + 1) & TABLE_MASK;
            }
        }

        private boolean matchNameRest(long base, long nameAddr, int nameLen, int idx) {
            if (nameLen <= 8) {
                return true;
            }
            int end = Math.min(nameLen, INLINE_MAX);
            int i = 8;
            for (; i + 8 <= end; i += 8) {
                if (UNSAFE.getLong(base + E_NAME + i)
                        != UNSAFE.getLong(nameAddr + i)) {
                    return false;
                }
            }
            for (; i < end; i++) {
                if (UNSAFE.getByte(base + E_NAME + i)
                        != UNSAFE.getByte(nameAddr + i)) {
                    return false;
                }
            }
            if (nameLen > INLINE_MAX) {
                byte[] key = keys[idx];
                for (i = INLINE_MAX; i + 8 <= nameLen; i += 8) {
                    if (UNSAFE.getLong(key, BYTE_ARRAY_BASE + i)
                            != UNSAFE.getLong(nameAddr + i)) {
                        return false;
                    }
                }
                for (; i < nameLen; i++) {
                    if (key[i] != UNSAFE.getByte(nameAddr + i)) {
                        return false;
                    }
                }
            }
            return true;
        }

        void mergeFrom(StationTable other) {
            for (int j = 0; j < other.occupiedCount; j++) {
                int i = other.occupied[j];
                long oBase = other.dataAddr + (long) i * ENTRY_BYTES;
                long packed = UNSAFE.getLong(oBase + E_PACKED);
                int nameLen = (int) packed;
                int hash = (int) (packed >>> 32);
                mergeEntry(other.keys[i], nameLen, hash, packed,
                        UNSAFE.getInt(oBase + E_MIN),
                        UNSAFE.getInt(oBase + E_MAX),
                        UNSAFE.getLong(oBase + E_SUM),
                        UNSAFE.getLong(oBase + E_COUNT));
            }
        }

        private void mergeEntry(byte[] name, int nameLen, int hash,
                long packedHashLen, int minVal, int maxVal, long sumVal, long countVal) {
            long firstWord = (nameLen >= 8)
                    ? UNSAFE.getLong(name, BYTE_ARRAY_BASE)
                    : readPartialWordArr(name, nameLen);
            int idx = hash & TABLE_MASK;
            while (true) {
                long base = dataAddr + (long) idx * ENTRY_BYTES;
                long stored = UNSAFE.getLong(base + E_PACKED);

                if (stored == 0) {
                    byte[] copy = new byte[nameLen];
                    System.arraycopy(name, 0, copy, 0, nameLen);
                    keys[idx] = copy;
                    UNSAFE.putLong(base + E_PACKED, packedHashLen);
                    UNSAFE.putLong(base + E_SUM, sumVal);
                    UNSAFE.putLong(base + E_COUNT, countVal);
                    UNSAFE.putInt(base + E_MIN, minVal);
                    UNSAFE.putInt(base + E_MAX, maxVal);
                    int copyLen = Math.min(nameLen, INLINE_MAX);
                    UNSAFE.copyMemory(name, BYTE_ARRAY_BASE, null, base + E_NAME, copyLen);
                    return;
                }

                if (stored == packedHashLen
                        && UNSAFE.getLong(base + E_NAME) == firstWord
                        && equalsNameBytes(keys[idx], name, nameLen)) {
                    UNSAFE.putInt(base + E_MIN,
                            Math.min(UNSAFE.getInt(base + E_MIN), minVal));
                    UNSAFE.putInt(base + E_MAX,
                            Math.max(UNSAFE.getInt(base + E_MAX), maxVal));
                    UNSAFE.putLong(base + E_SUM,
                            UNSAFE.getLong(base + E_SUM) + sumVal);
                    UNSAFE.putLong(base + E_COUNT,
                            UNSAFE.getLong(base + E_COUNT) + countVal);
                    return;
                }

                idx = (idx + 1) & TABLE_MASK;
            }
        }

        void forEach(EntryConsumer consumer) {
            for (int j = 0; j < occupiedCount; j++) {
                int i = occupied[j];
                long base = dataAddr + (long) i * ENTRY_BYTES;
                consumer.accept(keys[i], (int) UNSAFE.getLong(base + E_PACKED),
                        UNSAFE.getInt(base + E_MIN),
                        UNSAFE.getInt(base + E_MAX),
                        UNSAFE.getLong(base + E_SUM),
                        UNSAFE.getLong(base + E_COUNT));
            }
        }

        private static boolean equalsNameBytes(byte[] a, byte[] b, int len) {
            for (int i = 0; i < len; i++) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        }

        private static long readPartialWord(long addr, int len) {
            return UNSAFE.getLong(addr) & ((1L << (len << 3)) - 1);
        }

        private static long readPartialWordArr(byte[] arr, int len) {
            long word = 0;
            int n = Math.min(len, 8);
            for (int i = 0; i < n; i++) {
                word |= ((long) (arr[i] & 0xFF)) << (i << 3);
            }
            return word;
        }
    }
}
