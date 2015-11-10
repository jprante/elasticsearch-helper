package org.xbib.elasticsearch.common.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;

public class BufferPool implements Streamable {

    String name;
    long count;
    long totalCapacity;
    long used;

    BufferPool() {
    }

    public BufferPool(String name, long count, long totalCapacity, long used) {
        this.name = name;
        this.count = count;
        this.totalCapacity = totalCapacity;
        this.used = used;
    }

    public String getName() {
        return this.name;
    }

    public long getCount() {
        return this.count;
    }

    public ByteSizeValue getTotalCapacity() {
        return new ByteSizeValue(totalCapacity);
    }

    public ByteSizeValue getUsed() {
        return new ByteSizeValue(used);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        count = in.readLong();
        totalCapacity = in.readLong();
        used = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(count);
        out.writeLong(totalCapacity);
        out.writeLong(used);
    }
}
