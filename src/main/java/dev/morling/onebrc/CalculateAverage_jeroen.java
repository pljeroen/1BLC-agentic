package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class CalculateAverage_jeroen {

    private static final int READ_BUFFER_BYTES = 1 << 20;
    private static final long MAP_CHUNK_BYTES = 256L << 20;

    private CalculateAverage_jeroen() {
    }

    public static void main(String[] args) throws Exception {
        String file = args.length > 0 ? args[0] : "measurements.txt";
        int workers = args.length > 1
                ? Integer.parseInt(args[1])
                : Math.max(1, Runtime.getRuntime().availableProcessors());
        boolean printPhaseTimings = "1".equals(System.getenv("PRINT_PHASE_TIMINGS"));

        CalculationResult calculation = calculateResult(Path.of(file), workers);
        long formatStart = System.nanoTime();
        String formatted = format(calculation.statsByStation);
        long formatMs = (System.nanoTime() - formatStart) / 1_000_000;
        System.out.println(formatted);

        if (printPhaseTimings) {
            System.err.println("phase.split_ms=" + calculation.splitMs);
            System.err.println("phase.process_merge_ms=" + calculation.processMergeMs);
            System.err.println("phase.format_ms=" + formatMs);
            System.err.println("phase.total_ms=" + (calculation.splitMs + calculation.processMergeMs + formatMs));
        }
    }

    public static Map<String, Stats> calculate(Path file, int workers)
            throws IOException, ExecutionException, InterruptedException {
        return calculateResult(file, workers).statsByStation;
    }

    private static CalculationResult calculateResult(Path file, int workers)
            throws IOException, ExecutionException, InterruptedException {
        int safeWorkers = Math.max(1, workers);

        if (safeWorkers == 1) {
            long splitStart = System.nanoTime();
            long size = fileSize(file);
            long splitMs = (System.nanoTime() - splitStart) / 1_000_000;
            if (size == 0L) {
                return new CalculationResult(Map.of(), splitMs, 0L);
            }

            long processStart = System.nanoTime();
            StationTable table = ShardTask.processShard(file, new Shard(0L, size));
            Map<String, Stats> merged = materializeStringMap(table);
            long processMs = (System.nanoTime() - processStart) / 1_000_000;
            return new CalculationResult(merged, splitMs, processMs);
        }

        long splitStart = System.nanoTime();
        List<Shard> shards = splitIntoShards(file, safeWorkers);
        long splitMs = (System.nanoTime() - splitStart) / 1_000_000;
        if (shards.isEmpty()) {
            return new CalculationResult(Map.of(), splitMs, 0L);
        }

        long processStart = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(safeWorkers, shards.size()));
        try {
            List<Future<StationTable>> futures = new ArrayList<>();
            for (Shard shard : shards) {
                futures.add(pool.submit(new ShardTask(file, shard)));
            }

            List<StationTable> partials = new ArrayList<>(futures.size());
            for (Future<StationTable> future : futures) {
                partials.add(future.get());
            }

            StationTable mergedBytes = mergeTablesParallel(partials, safeWorkers);
            Map<String, Stats> merged = materializeStringMap(mergedBytes);
            long processMs = (System.nanoTime() - processStart) / 1_000_000;
            return new CalculationResult(merged, splitMs, processMs);
        }
        finally {
            pool.shutdown();
        }
    }

    private static StationTable mergeTablesParallel(List<StationTable> tables, int workers)
            throws ExecutionException, InterruptedException {
        if (tables.isEmpty()) {
            return new StationTable(1024);
        }
        if (tables.size() == 1) {
            return tables.get(0);
        }

        int mergeThreads = Math.max(1, Math.min(workers, tables.size() / 2));
        ExecutorService mergePool = Executors.newFixedThreadPool(mergeThreads);
        try {
            List<StationTable> current = tables;
            while (current.size() > 1) {
                List<Future<StationTable>> mergeFutures = new ArrayList<>((current.size() + 1) / 2);
                int i = 0;
                for (; i + 1 < current.size(); i += 2) {
                    StationTable left = current.get(i);
                    StationTable right = current.get(i + 1);
                    mergeFutures.add(mergePool.submit(() -> {
                        left.mergeFrom(right);
                        return left;
                    }));
                }

                List<StationTable> next = new ArrayList<>((current.size() + 1) / 2);
                for (Future<StationTable> future : mergeFutures) {
                    next.add(future.get());
                }
                if (i < current.size()) {
                    next.add(current.get(i));
                }
                current = next;
            }
            return current.get(0);
        }
        finally {
            mergePool.shutdown();
        }
    }

    private static long fileSize(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            return channel.size();
        }
    }

    private static Map<String, Stats> materializeStringMap(StationTable table) {
        Map<String, Stats> merged = new HashMap<>(4096);
        table.forEach((stationBytes, min, max, sum, count) -> {
            String station = new String(stationBytes, StandardCharsets.UTF_8);
            merged.put(station, Stats.fromAggregate(min, max, sum, count));
        });
        return merged;
    }

    public static String format(Map<String, Stats> statsByStation) {
        TreeMap<String, Stats> sorted = new TreeMap<>(statsByStation);
        StringBuilder out = new StringBuilder(Math.max(32, sorted.size() * 32));
        out.append('{');
        boolean first = true;

        for (Map.Entry<String, Stats> entry : sorted.entrySet()) {
            if (!first) {
                out.append(", ");
            }
            first = false;

            Stats s = entry.getValue();
            out.append(entry.getKey())
                    .append('=')
                    .append(formatTenths(s.minTenths))
                    .append('/')
                    .append(formatTenths(roundedMeanTenths(s.sumTenths, s.count)))
                    .append('/')
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

    static int parseTemperatureTenths(MappedByteBuffer buffer, int start, int endExclusive) {
        int i = start;
        int sign = 1;
        if (buffer.get(i) == '-') {
            sign = -1;
            i++;
        }

        int whole = 0;
        while (i < endExclusive && buffer.get(i) != '.') {
            whole = whole * 10 + (buffer.get(i) - '0');
            i++;
        }
        i++;
        int frac = i < endExclusive ? buffer.get(i) - '0' : 0;
        return sign * (whole * 10 + frac);
    }

    private static String formatTenths(long tenths) {
        long abs = Math.abs(tenths);
        long whole = abs / 10;
        long frac = abs % 10;
        return (tenths < 0 ? "-" : "") + whole + "." + frac;
    }

    private static List<Shard> splitIntoShards(Path file, int workers) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size == 0L) {
                return List.of();
            }

            List<Long> boundaries = new ArrayList<>();
            boundaries.add(0L);

            for (int i = 1; i < workers; i++) {
                long target = (size * i) / workers;
                long boundary = seekNextLineBoundary(channel, target, size);
                long last = boundaries.get(boundaries.size() - 1);
                if (boundary > last && boundary < size) {
                    boundaries.add(boundary);
                }
            }

            boundaries.add(size);

            List<Shard> shards = new ArrayList<>();
            for (int i = 0; i < boundaries.size() - 1; i++) {
                long start = boundaries.get(i);
                long end = boundaries.get(i + 1);
                if (end > start) {
                    shards.add(new Shard(start, end));
                }
            }
            return shards;
        }
    }

    private static long seekNextLineBoundary(FileChannel channel, long target, long size) throws IOException {
        if (target <= 0) {
            return 0;
        }
        if (target >= size) {
            return size;
        }

        ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
        long position = target;

        while (position < size) {
            buffer.clear();
            int toRead = (int) Math.min(buffer.capacity(), size - position);
            buffer.limit(toRead);

            int read = channel.read(buffer, position);
            if (read <= 0) {
                return size;
            }

            byte[] bytes = buffer.array();
            for (int i = 0; i < read; i++) {
                if (bytes[i] == '\n') {
                    return position + i + 1;
                }
            }

            position += read;
        }

        return size;
    }

    static final class Stats {
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
            s.minTenths = min;
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

    private record Shard(long start, long end) {
    }

    private record CalculationResult(
            Map<String, Stats> statsByStation,
            long splitMs,
            long processMergeMs) {
    }

    private interface StationEntryConsumer {
        void accept(byte[] stationBytes, int min, int max, long sum, long count);
    }

    private static final class StationTable {
        private static final float LOAD_FACTOR = 0.7f;

        private byte[][] keys;
        private int[] hashes;
        private int[] keyLens;
        private long[] keyPrefixes;
        private int[] min;
        private int[] max;
        private long[] sum;
        private long[] count;
        private int size;
        private int mask;
        private int threshold;

        private StationTable(int initialCapacityPow2) {
            int cap = 1;
            while (cap < initialCapacityPow2) {
                cap <<= 1;
            }
            keys = new byte[cap][];
            hashes = new int[cap];
            keyLens = new int[cap];
            keyPrefixes = new long[cap];
            min = new int[cap];
            max = new int[cap];
            sum = new long[cap];
            count = new long[cap];
            mask = cap - 1;
            threshold = (int) (cap * LOAD_FACTOR);
        }

        void accumulateWithHash(byte[] line, int start, int endExclusive, int valueTenths, int hash) {
            int len = endExclusive - start;
            long prefix = prefixBytes(line, start, endExclusive);
            accumulateWithHash(line, start, endExclusive, valueTenths, hash, len, prefix);
        }

        void accumulateMappedWithHash(MappedByteBuffer mapped, int start, int endExclusive, int valueTenths, int hash) {
            int len = endExclusive - start;
            long prefix = prefixMapped(mapped, start, endExclusive);
            if (size >= threshold) {
                rehash();
            }

            int idx = hash & mask;
            while (true) {
                byte[] key = keys[idx];
                if (key == null) {
                    byte[] station = new byte[len];
                    copyMappedRange(mapped, start, station, len);
                    keys[idx] = station;
                    hashes[idx] = hash;
                    keyLens[idx] = len;
                    keyPrefixes[idx] = prefix;
                    min[idx] = valueTenths;
                    max[idx] = valueTenths;
                    sum[idx] = valueTenths;
                    count[idx] = 1;
                    size++;
                    return;
                }

                if (hashes[idx] == hash
                        && keyLens[idx] == len
                        && keyPrefixes[idx] == prefix
                        && equalsMapped(key, mapped, start, len)) {
                    if (valueTenths < min[idx]) {
                        min[idx] = valueTenths;
                    }
                    if (valueTenths > max[idx]) {
                        max[idx] = valueTenths;
                    }
                    sum[idx] += valueTenths;
                    count[idx]++;
                    return;
                }

                idx = (idx + 1) & mask;
            }
        }

        private void accumulateWithHash(
                byte[] line,
                int start,
                int endExclusive,
                int valueTenths,
                int hash,
                int len,
                long prefix) {
            if (size >= threshold) {
                rehash();
            }

            int idx = hash & mask;

            while (true) {
                byte[] key = keys[idx];
                if (key == null) {
                    byte[] station = new byte[len];
                    System.arraycopy(line, start, station, 0, len);
                    keys[idx] = station;
                    hashes[idx] = hash;
                    keyLens[idx] = len;
                    keyPrefixes[idx] = prefix;
                    min[idx] = valueTenths;
                    max[idx] = valueTenths;
                    sum[idx] = valueTenths;
                    count[idx] = 1;
                    size++;
                    return;
                }

                if (hashes[idx] == hash
                        && keyLens[idx] == len
                        && keyPrefixes[idx] == prefix
                        && equalsBytes(key, line, start, len)) {
                    if (valueTenths < min[idx]) {
                        min[idx] = valueTenths;
                    }
                    if (valueTenths > max[idx]) {
                        max[idx] = valueTenths;
                    }
                    sum[idx] += valueTenths;
                    count[idx]++;
                    return;
                }

                idx = (idx + 1) & mask;
            }
        }

        void forEach(StationEntryConsumer consumer) {
            for (int i = 0; i < keys.length; i++) {
                byte[] key = keys[i];
                if (key != null) {
                    consumer.accept(key, min[i], max[i], sum[i], count[i]);
                }
            }
        }

        void mergeFrom(StationTable other) {
            for (int i = 0; i < other.keys.length; i++) {
                byte[] key = other.keys[i];
                if (key != null) {
                    accumulateAggregate(
                            key, other.hashes[i], other.keyLens[i], other.keyPrefixes[i],
                            other.min[i], other.max[i], other.sum[i], other.count[i]);
                }
            }
        }

        private void accumulateAggregate(
                byte[] keyBytes,
                int hash,
                int len,
                long prefix,
                int minValueTenths,
                int maxValueTenths,
                long sumTenthsValue,
                long countValue) {
            if (size >= threshold) {
                rehash();
            }

            int idx = hash & mask;

            while (true) {
                byte[] key = keys[idx];
                if (key == null) {
                    byte[] station = new byte[len];
                    System.arraycopy(keyBytes, 0, station, 0, len);
                    keys[idx] = station;
                    hashes[idx] = hash;
                    keyLens[idx] = len;
                    keyPrefixes[idx] = prefix;
                    min[idx] = minValueTenths;
                    max[idx] = maxValueTenths;
                    sum[idx] = sumTenthsValue;
                    count[idx] = countValue;
                    size++;
                    return;
                }

                if (hashes[idx] == hash
                        && keyLens[idx] == len
                        && keyPrefixes[idx] == prefix
                        && equalsBytes(key, keyBytes, 0, len)) {
                    if (minValueTenths < min[idx]) {
                        min[idx] = minValueTenths;
                    }
                    if (maxValueTenths > max[idx]) {
                        max[idx] = maxValueTenths;
                    }
                    sum[idx] += sumTenthsValue;
                    count[idx] += countValue;
                    return;
                }

                idx = (idx + 1) & mask;
            }
        }

        private void rehash() {
            byte[][] oldKeys = keys;
            int[] oldHashes = hashes;
            int[] oldKeyLens = keyLens;
            long[] oldKeyPrefixes = keyPrefixes;
            int[] oldMin = min;
            int[] oldMax = max;
            long[] oldSum = sum;
            long[] oldCount = count;

            int newCap = oldKeys.length << 1;
            keys = new byte[newCap][];
            hashes = new int[newCap];
            keyLens = new int[newCap];
            keyPrefixes = new long[newCap];
            min = new int[newCap];
            max = new int[newCap];
            sum = new long[newCap];
            count = new long[newCap];
            mask = newCap - 1;
            threshold = (int) (newCap * LOAD_FACTOR);
            size = 0;

            for (int i = 0; i < oldKeys.length; i++) {
                byte[] key = oldKeys[i];
                if (key != null) {
                    int hash = oldHashes[i];
                    int idx = hash & mask;
                    while (keys[idx] != null) {
                        idx = (idx + 1) & mask;
                    }
                    keys[idx] = key;
                    hashes[idx] = hash;
                    keyLens[idx] = oldKeyLens[i];
                    keyPrefixes[idx] = oldKeyPrefixes[i];
                    min[idx] = oldMin[i];
                    max[idx] = oldMax[i];
                    sum[idx] = oldSum[i];
                    count[idx] = oldCount[i];
                    size++;
                }
            }
        }

        private static int hashBytes(byte[] bytes, int start, int endExclusive) {
            int h = 0x811c9dc5;
            for (int i = start; i < endExclusive; i++) {
                h ^= (bytes[i] & 0xff);
                h *= 0x01000193;
            }
            return h;
        }

        private static boolean equalsBytes(byte[] key, byte[] line, int start, int len) {
            for (int i = 0; i < len; i++) {
                if (key[i] != line[start + i]) {
                    return false;
                }
            }
            return true;
        }

        private static boolean equalsMapped(byte[] key, MappedByteBuffer mapped, int start, int len) {
            for (int i = 0; i < len; i++) {
                if (key[i] != mapped.get(start + i)) {
                    return false;
                }
            }
            return true;
        }

        private static long prefixBytes(byte[] bytes, int start, int endExclusive) {
            int len = Math.min(8, endExclusive - start);
            long prefix = 0L;
            for (int i = 0; i < len; i++) {
                prefix |= ((long) bytes[start + i] & 0xffL) << (i * 8);
            }
            return prefix;
        }

        private static long prefixMapped(MappedByteBuffer mapped, int start, int endExclusive) {
            int len = Math.min(8, endExclusive - start);
            long prefix = 0L;
            for (int i = 0; i < len; i++) {
                prefix |= ((long) mapped.get(start + i) & 0xffL) << (i * 8);
            }
            return prefix;
        }

        private static void copyMappedRange(MappedByteBuffer mapped, int start, byte[] dest, int len) {
            for (int i = 0; i < len; i++) {
                dest[i] = mapped.get(start + i);
            }
        }
    }

    private static final class ShardTask implements Callable<StationTable> {
        private final Path file;
        private final Shard shard;

        private ShardTask(Path file, Shard shard) {
            this.file = file;
            this.shard = shard;
        }

        @Override
        public StationTable call() throws Exception {
            return processShard(file, shard);
        }

        static StationTable processShard(Path file, Shard shard) throws IOException {
            if ("1".equals(System.getenv("USE_MMAP"))) {
                return processShardMapped(file, shard);
            }
            return processShardRead(file, shard);
        }

        private static StationTable processShardRead(Path file, Shard shard) throws IOException {
            StationTable table = new StationTable(1024);
            byte[] carry = new byte[256];
            int carryLen = 0;

            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_BYTES);
                byte[] chunk = buffer.array();
                long position = shard.start;

                while (position < shard.end) {
                    int toRead = (int) Math.min(buffer.capacity(), shard.end - position);
                    buffer.clear();
                    buffer.limit(toRead);

                    int read = channel.read(buffer, position);
                    if (read <= 0) {
                        break;
                    }

                    position += read;
                    int segmentStart = 0;

                    for (int i = 0; i < read; i++) {
                        if (chunk[i] == '\n') {
                            if (carryLen == 0) {
                                parseSegment(chunk, segmentStart, i, table);
                            }
                            else {
                                int segmentLen = i - segmentStart;
                                carry = ensureCapacity(carry, carryLen + segmentLen);
                                System.arraycopy(chunk, segmentStart, carry, carryLen, segmentLen);
                                parseSegment(carry, 0, carryLen + segmentLen, table);
                                carryLen = 0;
                            }
                            segmentStart = i + 1;
                        }
                    }

                    if (segmentStart < read) {
                        int segmentLen = read - segmentStart;
                        carry = ensureCapacity(carry, carryLen + segmentLen);
                        System.arraycopy(chunk, segmentStart, carry, carryLen, segmentLen);
                        carryLen += segmentLen;
                    }
                }
            }

            if (carryLen > 0) {
                parseSegment(carry, 0, carryLen, table);
            }

            return table;
        }

        private static StationTable processShardMapped(Path file, Shard shard) throws IOException {
            StationTable table = new StationTable(1024);
            byte[] carry = new byte[256];
            int carryLen = 0;

            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                long position = shard.start;

                while (position < shard.end) {
                    long toMap = Math.min(MAP_CHUNK_BYTES, shard.end - position);
                    MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, position, toMap);
                    int read = (int) toMap;
                    int segmentStart = 0;

                    for (int i = 0; i < read; i++) {
                        if (mapped.get(i) == '\n') {
                            if (carryLen == 0) {
                                parseSegment(mapped, segmentStart, i, table);
                            }
                            else {
                                int segmentLen = i - segmentStart;
                                carry = ensureCapacity(carry, carryLen + segmentLen);
                                copyMappedRange(mapped, segmentStart, carry, carryLen, segmentLen);
                                parseSegment(carry, 0, carryLen + segmentLen, table);
                                carryLen = 0;
                            }
                            segmentStart = i + 1;
                        }
                    }

                    if (segmentStart < read) {
                        int segmentLen = read - segmentStart;
                        carry = ensureCapacity(carry, carryLen + segmentLen);
                        copyMappedRange(mapped, segmentStart, carry, carryLen, segmentLen);
                        carryLen += segmentLen;
                    }

                    position += toMap;
                }
            }

            if (carryLen > 0) {
                parseSegment(carry, 0, carryLen, table);
            }

            return table;
        }

        private static byte[] ensureCapacity(byte[] bytes, int required) {
            if (required <= bytes.length) {
                return bytes;
            }
            int newSize = bytes.length;
            while (newSize < required) {
                newSize <<= 1;
            }
            byte[] grown = new byte[newSize];
            System.arraycopy(bytes, 0, grown, 0, bytes.length);
            return grown;
        }

        private static void copyMappedRange(MappedByteBuffer mapped, int srcStart, byte[] dest, int destStart, int len) {
            for (int i = 0; i < len; i++) {
                dest[destStart + i] = mapped.get(srcStart + i);
            }
        }

        private static void parseSegment(MappedByteBuffer bytes, int start, int endExclusive, StationTable table) {
            while (endExclusive > start && bytes.get(endExclusive - 1) == '\r') {
                endExclusive--;
            }

            if (endExclusive - start <= 2) {
                return;
            }

            int sep = -1;
            int hash = 0x811c9dc5;
            for (int i = start; i < endExclusive; i++) {
                byte b = bytes.get(i);
                if (b == ';') {
                    sep = i;
                    break;
                }
                hash ^= (b & 0xff);
                hash *= 0x01000193;
            }

            if (sep <= start || sep >= endExclusive - 1) {
                return;
            }

            int valueTenths = parseTemperatureTenths(bytes, sep + 1, endExclusive);
            table.accumulateMappedWithHash(bytes, start, sep, valueTenths, hash);
        }

        private static void parseSegment(byte[] bytes, int start, int endExclusive, StationTable table) {
            while (endExclusive > start && bytes[endExclusive - 1] == '\r') {
                endExclusive--;
            }

            if (endExclusive - start <= 2) {
                return;
            }

            int sep = -1;
            int hash = 0x811c9dc5;
            for (int i = start; i < endExclusive; i++) {
                byte b = bytes[i];
                if (b == ';') {
                    sep = i;
                    break;
                }
                hash ^= (b & 0xff);
                hash *= 0x01000193;
            }

            if (sep <= start || sep >= endExclusive - 1) {
                return;
            }

            int valueTenths = parseTemperatureTenths(bytes, sep + 1, endExclusive);
            table.accumulateWithHash(bytes, start, sep, valueTenths, hash);
        }
    }
}
