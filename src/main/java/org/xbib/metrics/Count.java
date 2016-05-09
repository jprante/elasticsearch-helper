package org.xbib.metrics;

/**
 * An interface for metric types which have counts.
 */
public interface Count {

    void inc();

    void inc(long n);

    void inc(String index, String type, String id);

    void dec();

    void dec(long n);

    void dec(String index, String type, String id);

    /**
     * Returns the current count.
     *
     * @return the current count
     */
    long getCount();

    String getIncChecksum(String index, String type);

    String getDecChecksum(String index, String type);
}
