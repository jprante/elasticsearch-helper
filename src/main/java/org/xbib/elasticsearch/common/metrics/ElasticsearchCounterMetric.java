package org.xbib.elasticsearch.common.metrics;

import com.twitter.jsr166e.LongAdder;
import org.xbib.metrics.Count;

public class ElasticsearchCounterMetric implements Count {

    private final LongAdder counter = new LongAdder();

    @Override
    public void inc() {
        counter.increment();
    }

    @Override
    public void inc(long n) {
        counter.add(n);
    }

    @Override
    public void dec() {
        counter.decrement();
    }

    @Override
    public void dec(long n) {
        counter.add(-n);
    }

    @Override
    public long getCount() {
        return counter.sum();
    }
}
