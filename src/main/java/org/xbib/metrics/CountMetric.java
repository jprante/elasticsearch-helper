package org.xbib.metrics;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32;

/**
 * An incrementing and decrementing counter metric.
 */
public class CountMetric implements Metric, Count {

    private final LongAdder count;

    private final Map<String,CRC32> checksumIn;

    private final Map<String,CRC32> checksumOut;

    public CountMetric() {
        this.count = new LongAdder();
        this.checksumIn = new HashMap<>();
        this.checksumOut = new HashMap<>();
    }

    /**
     * Increment the counter by one.
     */
    @Override
    public void inc() {
        inc(1);
    }

    /**
     * Increment the counter by {@code n}.
     *
     * @param n the amount by which the counter will be increased
     */
    @Override
    public void inc(long n) {
        count.add(n);
    }

    @Override
    public void inc(String index, String type, String id) {
        CRC32 crc32 = checksumIn.get(index + "/" + type);
        if (crc32 == null) {
            crc32 = new CRC32();
            checksumIn.put(index + "/" + type, crc32);
        }
        if (id != null) {
            crc32.update(id.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Decrement the counter by one.
     */
    @Override
    public void dec() {
        dec(1);
    }

    /**
     * Decrement the counter by {@code n}.
     *
     * @param n the amount by which the counter will be decreased
     */
    @Override
    public void dec(long n) {
        count.add(-n);
    }

    @Override
    public void dec(String index, String type, String id) {
        CRC32 crc32 = checksumOut.get(index + "/" + type);
        if (crc32 == null) {
            crc32 = new CRC32();
            checksumOut.put(index + "/" + type, crc32);
        }
        if (id != null) {
            crc32.update(id.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Returns the counter's current value.
     *
     * @return the counter's current value
     */
    @Override
    public long getCount() {
        return count.sum();
    }

    @Override
    public String getIncChecksum(String index, String type) {
        return Long.toHexString(checksumIn.containsKey(index + "/" + type) ?
                checksumIn.get(index + "/" + type).getValue() : 0L);
    }

    @Override
    public String getDecChecksum(String index, String type) {
        return Long.toHexString(checksumOut.containsKey(index + "/" + type) ?
                checksumOut.get(index + "/" + type).getValue() : 0L);
    }
}
