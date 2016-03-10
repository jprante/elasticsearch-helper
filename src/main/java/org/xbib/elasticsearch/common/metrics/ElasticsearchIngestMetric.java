package org.xbib.elasticsearch.common.metrics;

import org.xbib.elasticsearch.helper.client.IngestMetric;
import org.xbib.metrics.Count;
import org.xbib.metrics.Metered;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ElasticsearchIngestMetric implements IngestMetric {

    private final Set<String> indexNames = new HashSet<>();
    private final Map<String, Long> startBulkRefreshIntervals = new HashMap<>();
    private final Map<String, Long> stopBulkRefreshIntervals = new HashMap<>();
    private final ElasticsearchMeterMetric totalIngest = new ElasticsearchMeterMetric();
    private final Count totalIngestSizeInBytes = new ElasticsearchCounterMetric();
    private final Count currentIngest = new ElasticsearchCounterMetric();
    private final Count currentIngestNumDocs = new ElasticsearchCounterMetric();
    private final Count submitted = new ElasticsearchCounterMetric();
    private final Count succeeded = new ElasticsearchCounterMetric();
    private final Count failed = new ElasticsearchCounterMetric();
    private Long started;
    private Long stopped;

    @Override
    public Metered getTotalIngest() {
        return totalIngest;
    }

    @Override
    public Count getTotalIngestSizeInBytes() {
        return totalIngestSizeInBytes;
    }

    @Override
    public Count getCurrentIngest() {
        return currentIngest;
    }

    @Override
    public Count getCurrentIngestNumDocs() {
        return currentIngestNumDocs;
    }

    @Override
    public Count getSubmitted() {
        return submitted;
    }

    @Override
    public Count getSucceeded() {
        return succeeded;
    }

    @Override
    public Count getFailed() {
        return failed;
    }

    @Override
    public ElasticsearchIngestMetric start() {
        this.started = System.nanoTime();
        totalIngest.spawn(5L);
        return this;
    }

    @Override
    public ElasticsearchIngestMetric stop() {
        this.stopped = System.nanoTime();
        totalIngest.stop();
        return this;
    }

    @Override
    public long elapsed() {
        return (stopped != null ? stopped : System.nanoTime()) - started;
    }

    @Override
    public ElasticsearchIngestMetric setupBulk(String indexName, long startRefreshInterval, long stopRefreshInterval) {
        synchronized (indexNames) {
            indexNames.add(indexName);
            startBulkRefreshIntervals.put(indexName, startRefreshInterval);
            stopBulkRefreshIntervals.put(indexName, stopRefreshInterval);
        }
        return this;
    }

    @Override
    public boolean isBulk(String indexName) {
        return indexNames.contains(indexName);
    }

    @Override
    public ElasticsearchIngestMetric removeBulk(String indexName) {
        synchronized (indexNames) {
            indexNames.remove(indexName);
        }
        return this;
    }

    @Override
    public Set<String> indices() {
        return indexNames;
    }

    @Override
    public Map<String, Long> getStartBulkRefreshIntervals() {
        return startBulkRefreshIntervals;
    }

    @Override
    public Map<String, Long> getStopBulkRefreshIntervals() {
        return stopBulkRefreshIntervals;
    }

}
