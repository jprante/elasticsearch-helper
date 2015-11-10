package org.xbib.elasticsearch.common.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class Classes implements Streamable {

    long loadedClassCount;
    long totalLoadedClassCount;
    long unloadedClassCount;

    Classes() {
    }

    Classes(long loadedClassCount, long totalLoadedClassCount, long unloadedClassCount) {
        this.loadedClassCount = loadedClassCount;
        this.totalLoadedClassCount = totalLoadedClassCount;
        this.unloadedClassCount = unloadedClassCount;
    }

    public long getLoadedClassCount() {
        return loadedClassCount;
    }

    public long getTotalLoadedClassCount() {
        return totalLoadedClassCount;
    }

    public long getUnloadedClassCount() {
        return unloadedClassCount;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        loadedClassCount = in.readLong();
        totalLoadedClassCount = in.readLong();
        unloadedClassCount = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(loadedClassCount);
        out.writeLong(totalLoadedClassCount);
        out.writeLong(unloadedClassCount);
    }
}