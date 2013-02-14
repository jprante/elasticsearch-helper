/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.support;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.IngestProcessor;
import org.elasticsearch.action.bulk.IngestRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransportClientIngest support
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class TransportClientIngestSupport extends TransportClientSearchSupport implements TransportClientIngest {

    private final static ESLogger logger = Loggers.getLogger(TransportClientIngestSupport.class);
    /**
     * The default size of a ingestProcessor request
     */
    private int maxBulkActions = 100;
    /**
     * The default number of maximum concurrent ingestProcessor requests
     */
    private int maxConcurrentBulkRequests = 30;
    /**
     * The outstanding ingestProcessor requests
     */
    private final AtomicLong outstandingBulkRequests = new AtomicLong();
    /**
     * Count the ingestProcessor volume
     */
    private final AtomicLong volumeCounter = new AtomicLong();
    /**
     * Is this ingesting enabled or not?
     */
    private boolean enabled = true;
    /**
     * The IngestProcessor
     */
    private IngestProcessor ingestProcessor;
    /**
     * The default type
     */
    private String type;
    /**
     * Date detection enabled or not?
     */
    private boolean dateDetection = false;
    /**
     * Optional settings
     */
    private ImmutableSettings.Builder settingsBuilder;
    /**
     * An optional mapping
     */
    private String mapping;

    /**
     * Enable or disable this indxer
     *
     * @param enabled true for enable, false for disable
     * @return this indexer
     */
    public TransportClientIngestSupport enable(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Is this indexer enabled?
     *
     * @return true if enabled, false if disabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Set the settings for new clients
     *
     * @param settings the settings
     * @return this indexer
     */
    @Override
    public TransportClientIngestSupport settings(Settings settings) {
        super.settings(settings);
        return this;
    }

    /**
     * Create a new client for this indexer
     *
     * @return this indexer
     */
    @Override
    public TransportClientIngestSupport newClient() {
        return newClient(findURI());
    }

    /**
     * Create new client with concurrent ingestProcessor processor.
     * <p/>
     * The URI describes host and port of the node the client should connect to,
     * with the parameter <tt>es.cluster.name</tt> for the cluster name.
     *
     * @param uri the cluster URI
     * @return this indexer
     */
    @Override
    public TransportClientIngestSupport newClient(URI uri) {
        super.newClient(uri);
        IngestProcessor.Listener listener = new IngestProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, IngestRequest request) {
                long l = outstandingBulkRequests.getAndIncrement();
                long v = volumeCounter.addAndGet(request.estimatedSizeInBytes());
                logger.info("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), v, l);
            }

            @Override
            public void afterBulk(long executionId, BulkResponse response) {
                long l = outstandingBulkRequests.decrementAndGet();
                logger.info("bulk [{}] success [{} items] [{}ms]",
                        executionId, response.items().length, response.took().millis());
            }

            @Override
            public void afterBulk(long executionId, Throwable failure) {
                long l = outstandingBulkRequests.decrementAndGet();
                logger.error("bulk [{}] error", executionId, failure);
                enabled = false;
            }
        };
        this.ingestProcessor = IngestProcessor.builder(client)
                .listener(listener)
                .actions(maxBulkActions)
                .concurrency(maxConcurrentBulkRequests)
                .build();
        this.enabled = true;
        return this;
    }

    /**
     * Initial settings tailored for index/ingestProcessor client use. Transport
     * sniffing, only thread pool is for ingestProcessor/indexing, other thread pools are
     * minimal, 4 * cpucore Netty connections in parallel.
     *
     * @param uri the cluster name URI
     * @return the initial settings
     */
    @Override
    protected Settings initialSettings(URI uri) {
        int n = Runtime.getRuntime().availableProcessors();
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("client.transport.sniff", true)
                .put("transport.netty.worker_count", n * 4)
                .put("transport.netty.connections_per_node.low", 0)
                .put("transport.netty.connections_per_node.med", 0)
                .put("transport.netty.connections_per_node.high", n * 4)
                .put("threadpool.index.type", "fixed")
                .put("threadpool.index.size", n * 4)
                .put("threadpool.ingestProcessor.type", "fixed")
                .put("threadpool.ingestProcessor.size", n * 4)
                .put("threadpool.get.type", "fixed")
                .put("threadpool.get.size", 1)
                .put("threadpool.search.type", "fixed")
                .put("threadpool.search.size", 1)
                .put("threadpool.percolate.type", "fixed")
                .put("threadpool.percolate.size", 1)
                .put("threadpool.management.type", "fixed")
                .put("threadpool.management.size", 1)
                .put("threadpool.flush.type", "fixed")
                .put("threadpool.flush.size", 1)
                .put("threadpool.merge.type", "fixed")
                .put("threadpool.merge.size", 1)
                .put("threadpool.refresh.type", "fixed")
                .put("threadpool.refresh.size", 1)
                .put("threadpool.cache.type", "fixed")
                .put("threadpool.cache.size", 1)
                .put("threadpool.snapshot.type", "fixed")
                .put("threadpool.snapshot.size", 1)
                .build();
    }

    @Override
    public TransportClientIngestSupport index(String index) {
        super.index(index);
        return this;
    }

    @Override
    public TransportClientIngestSupport type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public TransportClientIngestSupport dateDetection(boolean dateDetection) {
        this.dateDetection = dateDetection;
        return this;
    }

    @Override
    public TransportClientIngestSupport maxBulkActions(int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    @Override
    public TransportClientIngestSupport maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    public TransportClientIngestSupport setting(String key, String value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    public TransportClientIngestSupport setting(String key, Integer value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    public TransportClientIngestSupport mapping(String mapping) {
        this.mapping = mapping;
        return this;
    }

    public TransportClientIngestSupport newIndex() {
        return newIndex(true);
    }

    public synchronized TransportClientIngestSupport newIndex(boolean ignoreException) {
        if (client == null) {
            return this;
        }
        if (index() == null) {
            logger.warn("no index name given to create");
            return this;
        }
        if (type() == null) {
            logger.warn("no type name given to create");
            return this;
        }
        CreateIndexRequest request = new CreateIndexRequest(index());
        if (settingsBuilder != null) {
            request.settings(settingsBuilder);
        }
        if (mapping == null) {
            mapping = "{\"_default_\":{\"date_detection\":" + dateDetection + "}}";
        }
        request.mapping(type(), mapping);
        try {
            if (enabled) {
                logger.debug("index = {} type = {} settings = {} mapping = {}",
                        index(),
                        type(),
                        settingsBuilder.build().getAsMap(),
                        mapping
                );
                client.admin().indices().create(request).actionGet();
            }
        } catch (Exception e) {
            if (!ignoreException) {
                throw new RuntimeException(e);
            }
        }
        return this;
    }

    public TransportClientIngestSupport deleteIndex() {
        return deleteIndex(true);
    }

    public TransportClientIngestSupport deleteIndex(boolean ignoreException) {
        if (client == null) {
            return this;
        }
        if (index() == null) {
            return this;
        }
        try {
            if (enabled) {
                client.admin().indices().delete(new DeleteIndexRequest(index()));
            }
        } catch (Exception e) {
            if (!ignoreException) {
                throw new RuntimeException(e);
            }
        }
        return this;
    }

    public TransportClientIngestSupport newType(String mapping) {
        if (client == null) {
            return this;
        }
        if (index() == null) {
            return this;
        }
        client.admin().indices().putMapping(new PutMappingRequest()
                .indices(new String[]{index()})
                .type(type)
                .source(mapping))
                .actionGet();
        return this;
    }

    public TransportClientIngestSupport deleteType() {
        return deleteType(true, true);
    }

    public TransportClientIngestSupport deleteType(boolean enabled) {
        return deleteType(enabled, true);
    }

    public TransportClientIngestSupport deleteType(boolean enabled, boolean ignoreException) {
        if (client == null) {
            return this;
        }
        if (index() == null) {
            return this;
        }
        try {
            if (enabled) {
                client.admin().indices().deleteMapping(new DeleteMappingRequest()
                        .indices(new String[]{index()})
                        .type(type));
            }
        } catch (Exception e) {
            if (!ignoreException) {
                throw new RuntimeException(e);
            }
        }
        return this;
    }

    @Override
    public TransportClientIngestSupport startBulkMode() {
        disableRefreshInterval();
        return this;
    }

    @Override
    public TransportClientIngestSupport stopBulkMode() {
        enableRefreshInterval();
        return this;
    }

    public TransportClientIngestSupport refresh() {
        if (client == null) {
            return this;
        }
        if (index() == null) {
            return this;
        }
        client.admin().indices().refresh(new RefreshRequest().indices(index()));
        return this;
    }

    @Override
    public TransportClientIngestSupport create(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("create: {}/{}/{} source = {}", index, type, id, source);
        }
        IndexRequest indexRequest = Requests.indexRequest(index).type(type).id(id).create(true).source(source);
        try {
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            logger.error("ingestProcessor add of create failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public TransportClientIngestSupport index(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("index: {}/{}/{} source = {}", index, type, id, source);
        }
        IndexRequest indexRequest = Requests.indexRequest(index).type(type).id(id).create(false).source(source);
        try {
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            logger.error("ingestProcessor add of index failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public TransportClientIngestSupport delete(String index, String type, String id) {
        if (!enabled) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("delete: {}/{}/{} ", index, type, id);
        }
        DeleteRequest deleteRequest = Requests.deleteRequest(index).type(type).id(id);
        try {
            ingestProcessor.add(deleteRequest);
        } catch (Exception e) {
            logger.error("ingestProcessor add of delete failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public TransportClientIngestSupport waitForHealthyCluster() throws IOException {
        super.waitForHealthyCluster();
        return this;
    }

    @Override
    public TransportClientIngestSupport waitForHealthyCluster(ClusterHealthStatus status, String timeout) throws IOException {
        super.waitForHealthyCluster(status, timeout);
        return this;
    }

    public TransportClientIngestSupport numberOfShards(int value) {
        if (index() == null) {
            return this;
        }
        setting("index.number_of_shards", value);
        return this;
    }

    public TransportClientIngestSupport numberOfReplicas(int value) {
        if (index() == null) {
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        if (index() == null) {
            return -1;
        }
        super.waitForHealthyCluster(ClusterHealthStatus.YELLOW, "1m");
        super.update("number_of_replicas", level);
        return super.waitForRecovery();
    }

    @Override
    public TransportClientIngestSupport flush() {
        if (!enabled) {
            return this;
        }
        ingestProcessor.flush();
        return this;
    }

    @Override
    public synchronized void shutdown() {
        if (!enabled) {
            super.shutdown();
            return;
        }
        try {
            logger.info("closing...");
            ingestProcessor.close();
            logger.info("enable refresh interval...");
            enableRefreshInterval();
            logger.info("closed, shutting down...");
            super.shutdown();
            logger.info("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public long getVolumeInBytes() {
        return volumeCounter.get();
    }

}
