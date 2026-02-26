package dev.morling.onebrc;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
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
    private static final long SEGMENT_SIZE = 1L << 30; // 1 GB
    private static final int TABLE_SIZE = 1 << 14; // 16384
    private static final int TABLE_MASK = TABLE_SIZE - 1;
    private static final int SAFE_MARGIN = 128;
    private static final int MAX_LINE_LEN = 256;
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
    }

    public static Map<String, Stats> calculate(Path file, int workers)
            throws IOException, InterruptedException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize == 0) {
                return Map.of();
            }

            int segCount = (int) ((fileSize + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
            long[] segAddr = new long[segCount];
            long[] segLen = new long[segCount];
            long[] segMapLen = new long[segCount];
            MappedByteBuffer[] segBuf = new MappedByteBuffer[segCount];

            for (int s = 0; s < segCount; s++) {
                long off = (long) s * SEGMENT_SIZE;
                long nominalLen = Math.min(SEGMENT_SIZE, fileSize - off);
                long mapLen = Math.min(nominalLen + MAX_LINE_LEN, fileSize - off);
                segBuf[s] = channel.map(FileChannel.MapMode.READ_ONLY, off, mapLen);
                segAddr[s] = bufferAddress(segBuf[s]);
                segLen[s] = nominalLen;
                segMapLen[s] = mapLen;
            }

            int maxChunks = (int) (fileSize / CHUNK_SIZE) + segCount + 1;
            int[] chunkSeg = new int[maxChunks];
            long[] chunkStart = new long[maxChunks];
            long[] chunkEnd = new long[maxChunks];
            int numChunks = 0;

            for (int s = 0; s < segCount; s++) {
                long base = segAddr[s];
                long nomLen = segLen[s];
                long mapLen = segMapLen[s];
                long pos = 0;

                if (s > 0) {
                    while (pos < mapLen && UNSAFE.getByte(base + pos) != '\n') {
                        pos++;
                    }
                    if (pos < mapLen) {
                        pos++;
                    }
                }

                while (pos < nomLen) {
                    long end = Math.min(pos + CHUNK_SIZE, mapLen);
                    if (end < mapLen) {
                        while (end < mapLen && UNSAFE.getByte(base + end) != '\n') {
                            end++;
                        }
                        if (end < mapLen) {
                            end++;
                        }
                    }
                    chunkSeg[numChunks] = s;
                    chunkStart[numChunks] = pos;
                    chunkEnd[numChunks] = end;
                    numChunks++;
                    pos = end;
                }
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
                        long base = segAddr[chunkSeg[ci]];
                        processChunk(base + chunkStart[ci], base + chunkEnd[ci], table);
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

            for (MappedByteBuffer buf : segBuf) {
                if (buf != null) {
                    buf.isLoaded();
                }
            }

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
            int hash = 0;
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

            if (semiPos > 0) {
                long mask = (1L << (semiPos << 3)) - 1;
                hash = mixHash(hash, word & mask);
            }

            long semiAddr = addr + semiPos;
            int nameLen = (int) (semiAddr - nameAddr);

            long tempWord = UNSAFE.getLong(semiAddr + 1);
            int temp = parseTemperatureBranchless(tempWord);

            int dotBitPos = Long.numberOfTrailingZeros(~tempWord & 0x10101000);
            addr = semiAddr + 1 + (dotBitPos >>> 3) + 3;

            table.accumulate(nameAddr, nameLen, hash, temp);
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
        int hash = 0;
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

        table.accumulate(start, nameLen, hash, temp);
    }

    // ---- SWAR helpers ----

    private static long hasByte(long word, long pattern) {
        long xor = word ^ pattern;
        return (xor - 0x0101010101010101L) & ~xor & 0x8080808080808080L;
    }

    private static int mixHash(int hash, long word) {
        hash ^= (int) word;
        hash *= 0x01000193;
        hash ^= (int) (word >>> 32);
        hash *= 0x01000193;
        return hash;
    }

    static int parseTemperatureBranchless(long word) {
        long inv = ~word;
        int dotPos = Long.numberOfTrailingZeros(inv & 0x10101000);
        long signed = (inv << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        long digits = ((word & designMask) << (28 - dotPos)) & 0x0F000F0F00L;
        long absValue = ((digits * 0x640a0001L) >>> 32) & 0x3FF;
        return (int) ((absValue ^ signed) - signed);
    }

    // ---- Formatting ----

    private static String formatTenths(long tenths) {
        long abs = Math.abs(tenths);
        return (tenths < 0 ? "-" : "") + (abs / 10) + "." + (abs % 10);
    }

    private static long bufferAddress(MappedByteBuffer buf) {
        try {
            Field f = java.nio.Buffer.class.getDeclaredField("address");
            f.setAccessible(true);
            return f.getLong(buf);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private static final class StationTable {
        // AoS layout: 1 cache line (64 bytes) per slot.
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

        private final byte[] data = new byte[TABLE_SIZE * ENTRY_BYTES];
        private final byte[][] keys = new byte[TABLE_SIZE][];

        void accumulate(long nameAddr, int nameLen, int hash, int temp) {
            long firstWord = (nameLen >= 8)
                    ? UNSAFE.getLong(nameAddr)
                    : readPartialWord(nameAddr, nameLen);
            long packedHashLen = ((long) hash << 32) | (nameLen & 0xFFFFFFFFL);
            int idx = hash & TABLE_MASK;

            while (true) {
                long base = BYTE_ARRAY_BASE + (long) idx * ENTRY_BYTES;
                long stored = UNSAFE.getLong(data, base + E_PACKED);

                if (stored == packedHashLen
                        && UNSAFE.getLong(data, base + E_NAME) == firstWord
                        && matchNameRest(base, nameAddr, nameLen, idx)) {
                    UNSAFE.putLong(data, base + E_SUM,
                            UNSAFE.getLong(data, base + E_SUM) + temp);
                    UNSAFE.putLong(data, base + E_COUNT,
                            UNSAFE.getLong(data, base + E_COUNT) + 1);
                    int curMin = UNSAFE.getInt(data, base + E_MIN);
                    int curMax = UNSAFE.getInt(data, base + E_MAX);
                    if (temp < curMin) {
                        UNSAFE.putInt(data, base + E_MIN, temp);
                    }
                    if (temp > curMax) {
                        UNSAFE.putInt(data, base + E_MAX, temp);
                    }
                    return;
                }

                if (stored == 0) {
                    byte[] name = new byte[nameLen];
                    UNSAFE.copyMemory(null, nameAddr, name, BYTE_ARRAY_BASE, nameLen);
                    keys[idx] = name;
                    UNSAFE.putLong(data, base + E_PACKED, packedHashLen);
                    UNSAFE.putLong(data, base + E_SUM, temp);
                    UNSAFE.putLong(data, base + E_COUNT, 1);
                    UNSAFE.putInt(data, base + E_MIN, temp);
                    UNSAFE.putInt(data, base + E_MAX, temp);
                    int copyLen = Math.min(nameLen, INLINE_MAX);
                    UNSAFE.copyMemory(null, nameAddr, data, base + E_NAME, copyLen);
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
                if (UNSAFE.getLong(data, base + E_NAME + i)
                        != UNSAFE.getLong(nameAddr + i)) {
                    return false;
                }
            }
            for (; i < end; i++) {
                if (UNSAFE.getByte(data, base + E_NAME + i)
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
            for (int i = 0; i < TABLE_SIZE; i++) {
                long oBase = BYTE_ARRAY_BASE + (long) i * ENTRY_BYTES;
                long packed = UNSAFE.getLong(other.data, oBase + E_PACKED);
                if (packed != 0) {
                    int nameLen = (int) packed;
                    int hash = (int) (packed >>> 32);
                    mergeEntry(other.keys[i], nameLen, hash, packed,
                            UNSAFE.getInt(other.data, oBase + E_MIN),
                            UNSAFE.getInt(other.data, oBase + E_MAX),
                            UNSAFE.getLong(other.data, oBase + E_SUM),
                            UNSAFE.getLong(other.data, oBase + E_COUNT));
                }
            }
        }

        private void mergeEntry(byte[] name, int nameLen, int hash,
                long packedHashLen, int minVal, int maxVal, long sumVal, long countVal) {
            long firstWord = (nameLen >= 8)
                    ? UNSAFE.getLong(name, BYTE_ARRAY_BASE)
                    : readPartialWordArr(name, nameLen);
            int idx = hash & TABLE_MASK;
            while (true) {
                long base = BYTE_ARRAY_BASE + (long) idx * ENTRY_BYTES;
                long stored = UNSAFE.getLong(data, base + E_PACKED);

                if (stored == 0) {
                    byte[] copy = new byte[nameLen];
                    System.arraycopy(name, 0, copy, 0, nameLen);
                    keys[idx] = copy;
                    UNSAFE.putLong(data, base + E_PACKED, packedHashLen);
                    UNSAFE.putLong(data, base + E_SUM, sumVal);
                    UNSAFE.putLong(data, base + E_COUNT, countVal);
                    UNSAFE.putInt(data, base + E_MIN, minVal);
                    UNSAFE.putInt(data, base + E_MAX, maxVal);
                    int copyLen = Math.min(nameLen, INLINE_MAX);
                    UNSAFE.copyMemory(name, BYTE_ARRAY_BASE, data, base + E_NAME, copyLen);
                    return;
                }

                if (stored == packedHashLen
                        && UNSAFE.getLong(data, base + E_NAME) == firstWord
                        && equalsNameBytes(keys[idx], name, nameLen)) {
                    UNSAFE.putInt(data, base + E_MIN,
                            Math.min(UNSAFE.getInt(data, base + E_MIN), minVal));
                    UNSAFE.putInt(data, base + E_MAX,
                            Math.max(UNSAFE.getInt(data, base + E_MAX), maxVal));
                    UNSAFE.putLong(data, base + E_SUM,
                            UNSAFE.getLong(data, base + E_SUM) + sumVal);
                    UNSAFE.putLong(data, base + E_COUNT,
                            UNSAFE.getLong(data, base + E_COUNT) + countVal);
                    return;
                }

                idx = (idx + 1) & TABLE_MASK;
            }
        }

        void forEach(EntryConsumer consumer) {
            for (int i = 0; i < TABLE_SIZE; i++) {
                long base = BYTE_ARRAY_BASE + (long) i * ENTRY_BYTES;
                long packed = UNSAFE.getLong(data, base + E_PACKED);
                if (packed != 0) {
                    consumer.accept(keys[i], (int) packed,
                            UNSAFE.getInt(data, base + E_MIN),
                            UNSAFE.getInt(data, base + E_MAX),
                            UNSAFE.getLong(data, base + E_SUM),
                            UNSAFE.getLong(data, base + E_COUNT));
                }
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
            long word = 0;
            int n = Math.min(len, 8);
            for (int i = 0; i < n; i++) {
                word |= ((long) (UNSAFE.getByte(addr + i) & 0xFF)) << (i << 3);
            }
            return word;
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
