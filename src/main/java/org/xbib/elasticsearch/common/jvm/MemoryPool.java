package org.xbib.elasticsearch.common.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;

public class MemoryPool implements Streamable {

    String name;
    long used;
    long max;

    long peakUsed;
    long peakMax;

    MemoryPool() {

    }

    public MemoryPool(String name, long used, long max, long peakUsed, long peakMax) {
        this.name = name;
        this.used = used;
        this.max = max;
        this.peakUsed = peakUsed;
        this.peakMax = peakMax;
    }

    public static MemoryPool readMemoryPool(StreamInput in) throws IOException {
        MemoryPool pool = new MemoryPool();
        pool.readFrom(in);
        return pool;
    }

    public String getName() {
        return this.name;
    }

    public ByteSizeValue getUsed() {
        return new ByteSizeValue(used);
    }

    public ByteSizeValue getMax() {
        return new ByteSizeValue(max);
    }

    public ByteSizeValue getPeakUsed() {
        return new ByteSizeValue(peakUsed);
    }

    public ByteSizeValue getPeakMax() {
        return new ByteSizeValue(peakMax);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        used = in.readVLong();
        max = in.readVLong();
        peakUsed = in.readVLong();
        peakMax = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(used);
        out.writeVLong(max);
        out.writeVLong(peakUsed);
        out.writeVLong(peakMax);
    }
}
