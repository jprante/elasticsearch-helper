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
