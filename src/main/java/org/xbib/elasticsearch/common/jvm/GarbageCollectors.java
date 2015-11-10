package org.xbib.elasticsearch.common.jvm;

import com.google.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Iterator;

public class GarbageCollectors implements Streamable, Iterable<GarbageCollector> {

    GarbageCollector[] collectors;

    GarbageCollectors() {
    }

    public static GarbageCollectors readGarbageCollectors(StreamInput in) throws IOException {
        GarbageCollectors collectors = new GarbageCollectors();
        collectors.readFrom(in);
        return collectors;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        collectors = new GarbageCollector[in.readVInt()];
        for (int i = 0; i < collectors.length; i++) {
            collectors[i] = GarbageCollector.readGarbageCollector(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(collectors.length);
        for (GarbageCollector gc : collectors) {
            gc.writeTo(out);
        }
    }

    public GarbageCollector[] getCollectors() {
        return this.collectors;
    }

    @Override
    public Iterator<GarbageCollector> iterator() {
        return Iterators.forArray(collectors);
    }
}

