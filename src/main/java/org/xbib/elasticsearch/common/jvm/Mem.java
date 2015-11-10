package org.xbib.elasticsearch.common.jvm;

import com.google.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Iterator;

public class Mem implements Streamable, Iterable<MemoryPool> {

    long heapInit;
    long heapCommitted;
    long heapUsed;
    long heapMax;
    long nonHeapInit;
    long nonHeapCommitted;
    long nonHeapUsed;
    long nonHeapMax;
    long directMemoryMax;

    MemoryPool[] pools = new MemoryPool[0];

    Mem() {
    }

    public static Mem readMem(StreamInput in) throws IOException {
        Mem mem = new Mem();
        mem.readFrom(in);
        return mem;
    }

    @Override
    public Iterator<MemoryPool> iterator() {
        return Iterators.forArray(pools);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        heapInit = in.readVLong();
        heapCommitted = in.readVLong();
        heapUsed = in.readVLong();
        heapMax = in.readVLong();
        nonHeapInit = in.readVLong();
        nonHeapCommitted = in.readVLong();
        nonHeapUsed = in.readVLong();
        nonHeapMax = in.readVLong();
        directMemoryMax = in.readVLong();
        pools = new MemoryPool[in.readVInt()];
        for (int i = 0; i < pools.length; i++) {
            pools[i] = MemoryPool.readMemoryPool(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(heapInit);
        out.writeVLong(heapCommitted);
        out.writeVLong(heapUsed);
        out.writeVLong(heapMax);
        out.writeVLong(nonHeapInit);
        out.writeVLong(nonHeapCommitted);
        out.writeVLong(nonHeapUsed);
        out.writeVLong(nonHeapMax);
        out.writeVLong(directMemoryMax);
        out.writeVInt(pools.length);
        for (MemoryPool pool : pools) {
            pool.writeTo(out);
        }
    }

    public ByteSizeValue getHeapInit() {
        return new ByteSizeValue(heapInit);
    }

    public ByteSizeValue getHeapCommitted() {
        return new ByteSizeValue(heapCommitted);
    }

    public ByteSizeValue getHeapUsed() {
        return new ByteSizeValue(heapUsed);
    }

    public ByteSizeValue getHeapMax() {
        return new ByteSizeValue(heapMax);
    }

    public ByteSizeValue getNonHeapInit() {
        return new ByteSizeValue(nonHeapInit);
    }

    public ByteSizeValue getNonHeapCommitted() {
        return new ByteSizeValue(nonHeapCommitted);
    }

    public ByteSizeValue getNonHeapUsed() {
        return new ByteSizeValue(nonHeapUsed);
    }

    public ByteSizeValue getNonHeapMax() {
        return new ByteSizeValue(nonHeapMax);
    }

    public ByteSizeValue getDirectMemoryMax() {
        return new ByteSizeValue(directMemoryMax);
    }

    public short getHeapUsedPercent() {
        if (heapMax == 0) {
            return -1;
        }
        return (short) (heapUsed * 100 / heapMax);
    }

}
