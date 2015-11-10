package org.xbib.elasticsearch.common.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class Threads implements Streamable {

    int count;
    int peakCount;

    Threads() {
    }

    public static Threads readThreads(StreamInput in) throws IOException {
        Threads threads = new Threads();
        threads.readFrom(in);
        return threads;
    }

    public int getCount() {
        return count;
    }

    public int getPeakCount() {
        return peakCount;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVInt();
        peakCount = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(count);
        out.writeVInt(peakCount);
    }
}