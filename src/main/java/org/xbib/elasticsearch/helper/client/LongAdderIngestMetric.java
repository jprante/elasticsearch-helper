/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.helper.client;

import org.xbib.metrics.CounterMetric;
import org.xbib.metrics.LongAdderCounterMetric;
import org.xbib.metrics.LongAdderMeanMetric;
import org.xbib.metrics.MeanMetric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LongAdderIngestMetric implements IngestMetric {

    private final Set<String> indexNames = new HashSet<>();

    private final Map<String, Long> startBulkRefreshIntervals = new HashMap<>();

    private final Map<String, Long> stopBulkRefreshIntervals = new HashMap<>();

    private final MeanMetric totalIngest = new LongAdderMeanMetric();

    private final CounterMetric totalIngestSizeInBytes = new LongAdderCounterMetric();

    private final CounterMetric currentIngest = new LongAdderCounterMetric();

    private final CounterMetric currentIngestNumDocs = new LongAdderCounterMetric();

    private final CounterMetric submitted = new LongAdderCounterMetric();

    private final CounterMetric succeeded = new LongAdderCounterMetric();

    private final CounterMetric failed = new LongAdderCounterMetric();

    private long started;

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

    public LongAdderIngestMetric start() {
        this.started = System.nanoTime();
        return this;
    }

    public long elapsed() {
        return System.nanoTime() - started;
    }

    public LongAdderIngestMetric setupBulk(String indexName, long startRefreshInterval, long stopRefreshInterval) {
        synchronized (indexNames) {
            indexNames.add(indexName);
            startBulkRefreshIntervals.put(indexName, startRefreshInterval);
            stopBulkRefreshIntervals.put(indexName, stopRefreshInterval);
        }
        return this;
    }

    public boolean isBulk(String indexName) {
        return indexNames.contains(indexName);
    }

    public LongAdderIngestMetric removeBulk(String indexName) {
        synchronized (indexNames) {
            indexNames.remove(indexName);
        }
        return this;
    }

    public Set<String> indices() {
        return indexNames;
    }

    public Map<String, Long> getStartBulkRefreshIntervals() {
        return startBulkRefreshIntervals;
    }

    public Map<String, Long> getStopBulkRefreshIntervals() {
        return stopBulkRefreshIntervals;
    }

}
