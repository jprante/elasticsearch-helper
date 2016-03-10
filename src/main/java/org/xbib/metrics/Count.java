package org.xbib.metrics;

/**
 * An interface for metric types which have counts.
 */
public interface Count {

    void inc();

    void inc(long n);

    void dec();

    void dec(long n);

    /**
     * Returns the current count.
     *
     * @return the current count
     */
    long getCount();
}
