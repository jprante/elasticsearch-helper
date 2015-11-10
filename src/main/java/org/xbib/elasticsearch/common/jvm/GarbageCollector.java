package org.xbib.elasticsearch.common.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class GarbageCollector implements Streamable {

    String name;
    long collectionCount;
    long collectionTime;

    GarbageCollector() {
    }

    public static GarbageCollector readGarbageCollector(StreamInput in) throws IOException {
        GarbageCollector gc = new GarbageCollector();
        gc.readFrom(in);
        return gc;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        collectionCount = in.readVLong();
        collectionTime = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(collectionCount);
        out.writeVLong(collectionTime);
    }

    public String getName() {
        return this.name;
    }

    public long getCollectionCount() {
        return this.collectionCount;
    }

    public TimeValue getCollectionTime() {
        return new TimeValue(collectionTime, TimeUnit.MILLISECONDS);
    }
}
