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

import org.xbib.metrics.Count;
import org.xbib.metrics.CountMetric;
import org.xbib.metrics.Meter;
import org.xbib.metrics.Metered;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LongAdderIngestMetric implements IngestMetric {

    private final Set<String> indexNames = new HashSet<>();

    private final Map<String, Long> startBulkRefreshIntervals = new HashMap<>();

    private final Map<String, Long> stopBulkRefreshIntervals = new HashMap<>();

    private final Meter totalIngest = new Meter();

    private final Count totalIngestSizeInBytes = new CountMetric();

    private final Count currentIngest = new CountMetric();

    private final Count currentIngestNumDocs = new CountMetric();

    private final Count submitted = new CountMetric();

    private final Count succeeded = new CountMetric();

    private final Count failed = new CountMetric();

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
    public LongAdderIngestMetric start() {
        this.started = System.nanoTime();
        this.totalIngest.spawn(5L);
        return this;
    }

    @Override
    public LongAdderIngestMetric stop() {
        this.stopped = System.nanoTime();
        totalIngest.stop();
        return this;
    }

    @Override
    public long elapsed() {
        return (stopped != null ? stopped : System.nanoTime()) - started;
    }

    @Override
    public LongAdderIngestMetric setupBulk(String indexName, long startRefreshInterval, long stopRefreshInterval) {
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
    public LongAdderIngestMetric removeBulk(String indexName) {
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
