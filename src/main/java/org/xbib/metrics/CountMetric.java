package org.xbib.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * An incrementing and decrementing counter metric.
 */
public class CountMetric implements Metric, Count {

    private final LongAdder count;

    public CountMetric() {
        this.count = new LongAdder();
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

    /**
     * Returns the counter's current value.
     *
     * @return the counter's current value
     */
    @Override
    public long getCount() {
        return count.sum();
    }
}
