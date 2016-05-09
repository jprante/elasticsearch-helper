package org.xbib.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * A metric which calculates the distribution of a value.
 *
 * @see <a href="http://www.johndcook.com/standard_deviation.html">Accurately computing running
 * variance</a>
 */
public class Histogram implements Metric, Sampling, Count {
    private final Reservoir reservoir;
    private final LongAdder count;

    /**
     * Creates a new {@link Histogram} with the given reservoir.
     *
     * @param reservoir the reservoir to create a histogram from
     */
    public Histogram(Reservoir reservoir) {
        this.reservoir = reservoir;
        this.count = new LongAdder();
    }

    @Override
    public void inc() {
        inc(1);
    }

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    @Override
    public void inc(long value) {
        count.increment();
        reservoir.update(value);
    }

    @Override
    public void inc(String index, String type, String id) {

    }

    @Override
    public void dec() {
        dec(1);
    }

    @Override
    public void dec(long n) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dec(String index, String type, String id) {

    }

    /**
     * Returns the number of values recorded.
     *
     * @return the number of values recorded
     */
    @Override
    public long getCount() {
        return count.sum();
    }

    @Override
    public String getIncChecksum(String index, String type) {
        return null;
    }

    @Override
    public String getDecChecksum(String index, String type) {
        return null;
    }

    @Override
    public Snapshot getSnapshot() {
        return reservoir.getSnapshot();
    }
}
