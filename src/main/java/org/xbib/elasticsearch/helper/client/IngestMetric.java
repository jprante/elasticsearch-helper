package org.xbib.elasticsearch.helper.client;

import org.xbib.metrics.CounterMetric;
import org.xbib.metrics.MeanMetric;

import java.util.Map;
import java.util.Set;

public interface IngestMetric {

    MeanMetric getTotalIngest();

    CounterMetric getTotalIngestSizeInBytes();

    CounterMetric getCurrentIngest();

    CounterMetric getCurrentIngestNumDocs();

    CounterMetric getSubmitted();

    CounterMetric getSucceeded();

    CounterMetric getFailed();

    IngestMetric start() ;

    long elapsed();

    IngestMetric setupBulk(String indexName, long startRefreshInterval, long stopRefreshInterval);

    boolean isBulk(String indexName);

    IngestMetric removeBulk(String indexName);

    Set<String> indices();

    Map<String, Long> getStartBulkRefreshIntervals();

    Map<String, Long> getStopBulkRefreshIntervals();

}
