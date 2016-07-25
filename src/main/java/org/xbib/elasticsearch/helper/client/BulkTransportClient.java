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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Transport client using the BulkProcessor of Elasticsearch
 */
public class BulkTransportClient extends BaseMetricTransportClient implements ClientAPI {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportClient.class.getName());

    private int maxActionsPerRequest = DEFAULT_MAX_ACTIONS_PER_REQUEST;

    private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;

    private ByteSizeValue maxVolumePerRequest = DEFAULT_MAX_VOLUME_PER_REQUEST;

    private TimeValue flushInterval = DEFAULT_FLUSH_INTERVAL;

    private BulkProcessor bulkProcessor;

    private Throwable throwable;

    private boolean closed;

    BulkTransportClient() {
    }

    @Override
    public BulkTransportClient maxActionsPerRequest(int maxActionsPerRequest) {
        this.maxActionsPerRequest = maxActionsPerRequest;
        return this;
    }

    @Override
    public BulkTransportClient maxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    @Override
    public BulkTransportClient maxVolumePerRequest(ByteSizeValue maxVolumePerRequest) {
        this.maxVolumePerRequest = maxVolumePerRequest;
        return this;
    }

    @Override
    public BulkTransportClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public BulkTransportClient init(ElasticsearchClient client, IngestMetric metric) throws IOException {
        return this.init(findSettings(), metric);
    }

    @Override
    public BulkTransportClient init(Settings settings, final IngestMetric metric) {
        super.init(settings, metric);
        resetSettings();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                metric.getCurrentIngest().inc();
                long l = metric.getCurrentIngest().getCount();
                int n = request.numberOfActions();
                metric.getSubmitted().inc(n);
                metric.getCurrentIngestNumDocs().inc(n);
                metric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                metric.getCurrentIngest().dec();
                long l = metric.getCurrentIngest().getCount();
                metric.getSucceeded().inc(response.getItems().length);
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    metric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
                    if (itemResponse.isFailed()) {
                        n++;
                        metric.getSucceeded().dec(1);
                        metric.getFailed().inc(1);
                    }
                }
                logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] [concurrent requests={}]",
                        executionId,
                        metric.getSucceeded().getCount(),
                        metric.getFailed().getCount(),
                        response.getTook().millis(),
                        l);
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    metric.getCurrentIngestNumDocs().dec(response.getItems().length);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest requst, Throwable failure) {
                metric.getCurrentIngest().dec();
                throwable = failure;
                if (!ignoreBulkErrors) {
                    closed = true;
                }
                logger.error("bulk [" + executionId + "] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxActionsPerRequest)
                .setConcurrentRequests(maxConcurrentRequests)
                .setFlushInterval(flushInterval);
        if (maxVolumePerRequest != null) {
            builder.setBulkSize(maxVolumePerRequest);
        }
        this.bulkProcessor = builder.build();
        try {
            Collection<InetSocketTransportAddress> addrs = findAddresses(settings);
            if (!connect(addrs, settings.getAsBoolean("autodiscover", false))) {
                throw new NoNodeAvailableException("no cluster nodes available, check settings "
                        + settings.getAsMap());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        this.closed = false;
        return this;
    }

    @Override
    public ElasticsearchClient client() {
        return client;
    }

    @Override
    public IngestMetric getMetric() {
        return metric;
    }

    @Override
    public BulkTransportClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        super.newIndex(index);
        return this;
    }

    @Override
    public BulkTransportClient newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        super.newIndex(index, settings, mappings);
        return this;
    }

    @Override
    public BulkTransportClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        super.deleteIndex(index);
        return this;
    }

    @Override
    public BulkTransportClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) throws IOException {
        super.startBulk(index, startRefreshInterval, stopRefreshIterval);
        return this;
    }

    @Override
    public BulkTransportClient stopBulk(String index) throws IOException {
        super.stopBulk(index);
        return this;
    }

    @Override
    public BulkTransportClient index(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc(index, type, id);
            bulkProcessor.add(new IndexRequest().index(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc(indexRequest.index(), indexRequest.type(), indexRequest.id());
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient delete(String index, String type, String id) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc(index, type, id);
            bulkProcessor.add(new DeleteRequest().index(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient update(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc(index, type, id);
            bulkProcessor.add(new UpdateRequest().index(index).type(type).id(id).upsert(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkUpdate(UpdateRequest updateRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc(updateRequest.index(), updateRequest.type(), updateRequest.id());
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public synchronized BulkTransportClient flushIngest() {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        logger.debug("flushing bulk processor");
        bulkProcessor.flush();
        return this;
    }

    @Override
    public synchronized BulkTransportClient waitForResponses(TimeValue maxWaitTime) throws InterruptedException, ExecutionException {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        bulkProcessor.awaitClose(maxWaitTime.getMillis(), TimeUnit.MILLISECONDS);
        return this;
    }

    @Override
    public synchronized void shutdown() {
        if (closed) {
            super.shutdown();
            throw new ElasticsearchException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return;
        }
        try {
            if (bulkProcessor != null) {
                logger.debug("closing bulk processor...");
                bulkProcessor.close();
            }
            if (metric != null && metric.indices() != null && !metric.indices().isEmpty()) {
                logger.debug("stopping bulk mode for indices {}...", metric.indices());
                for (String index : ImmutableSet.copyOf(metric.indices())) {
                    stopBulk(index);
                }
                metric.stop();
            }
            logger.debug("shutting down...");
            super.shutdown();
            logger.debug("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

}
