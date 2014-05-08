
package org.xbib.elasticsearch.support.client;

import org.xbib.metrics.CounterMetric;
import org.xbib.metrics.MeanMetric;

public class State {

    private String indexName;

    private boolean bulkMode;

    private final MeanMetric totalIngest = new MeanMetric();

    private final CounterMetric totalIngestSizeInBytes = new CounterMetric();

    private final CounterMetric currentIngest = new CounterMetric();

    private final CounterMetric currentIngestNumDocs = new CounterMetric();

    private final CounterMetric submitted = new CounterMetric();

    private final CounterMetric succeeded = new CounterMetric();

    private final CounterMetric failed = new CounterMetric();

    public State setIndexName(String name) {
        this.indexName = name;
        return this;
    }

    public String getIndexName() {
        return indexName;
    }

    public State setBulk(boolean enabled) {
        this.bulkMode = enabled;
        return this;
    }

    public boolean isBulk() {
        return bulkMode;
    }

    public MeanMetric getTotalIngest() {
        return totalIngest;
    }

    public CounterMetric getTotalIngestSizeInBytes() {
        return totalIngestSizeInBytes;
    }

    public CounterMetric getCurrentIngest() {
        return currentIngest;
    }

    public CounterMetric getCurrentIngestNumDocs() {
        return currentIngestNumDocs;
    }

    public CounterMetric getSubmitted() {
        return submitted;
    }

    public CounterMetric getSucceeded() {
        return succeeded;
    }

    public CounterMetric getFailed() {
        return failed;
    }


}
