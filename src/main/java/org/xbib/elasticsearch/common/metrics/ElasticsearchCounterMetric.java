package org.xbib.elasticsearch.common.metrics;

import com.twitter.jsr166e.LongAdder;
import org.xbib.metrics.Count;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class ElasticsearchCounterMetric implements Count {

    private final LongAdder count;

    private final Map<String,CRC32> checksumIn;

    private final Map<String,CRC32> checksumOut;

    ElasticsearchCounterMetric() {
        this.count = new LongAdder();
        this.checksumIn = new HashMap<>();
        this.checksumOut = new HashMap<>();
    }

    @Override
    public void inc() {
        count.increment();
    }

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

    @Override
    public void dec() {
        count.decrement();
    }

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
