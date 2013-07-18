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
package org.xbib.elasticsearch.support.ingest.transport;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.xbib.elasticsearch.support.TransportClientIngest;
import org.xbib.elasticsearch.support.TransportClientSupport;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransportClientIngest support
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public class TransportClientIngestSupport extends TransportClientSupport implements TransportClientIngest {

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
    private final AtomicLong outstandingRequests = new AtomicLong();
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
     * The default index
     */
    private String index;
    /**
     * The default type
     */
    private String type;

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
     * Create a new client for this indexer
     *
     * @return this indexer
     */
    @Override
    public TransportClientIngestSupport newClient() {
        return this.newClient(findURI());
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
                long l = outstandingRequests.getAndIncrement();
                long v = volumeCounter.addAndGet(request.estimatedSizeInBytes());
                logger.info("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), v, l);
            }

            @Override
            public void afterBulk(long executionId, IngestResponse response) {
                long l = outstandingRequests.decrementAndGet();
                logger.info("bulk [{}] [{} items succeeded] [{} items failed] [{}ms]",
                        executionId, response.success().size(), response.failure().size(), response.took().millis());
                if (!response.failure().isEmpty()) {
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("bulk [{}] [{} failure reason: {}", executionId, f.id(), f.message());
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, Throwable failure) {
                long l = outstandingRequests.decrementAndGet();
                logger.error("bulk ["+executionId+"] error", failure);
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

    @Override
    public Client client() {
        return client;
    }

    /**
     * Initial settings tailored for index/ingestProcessor client use. Transport
     * sniffing, only thread pool is for ingestProcessor/indexing, other thread pools are
     * minimal, 4 * cpucore Netty connections in parallel.
     *
     * @param uri the cluster name URI
     * @param n the client thread pool size
     * @return the initial settings
     */
    @Override
    protected Settings initialSettings(URI uri, int n) {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false) // sniff would join us into any cluster ... bug?
                .put("transport.netty.worker_count", n)
                .put("transport.netty.connections_per_node.low", 0)
                .put("transport.netty.connections_per_node.med", 0)
                .put("transport.netty.connections_per_node.high", n)
                .put("threadpool.index.type", "fixed")
                .put("threadpool.index.size", n)
                .put("threadpool.bulk.type", "fixed")
                .put("threadpool.bulk.size", n)
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
    public TransportClientIngestSupport setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public TransportClientIngestSupport setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
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
        super.setting(key, value);
        return this;
    }

    public TransportClientIngestSupport setting(String key, Integer value) {
        super.setting(key, value);
        return this;
    }

    public TransportClientIngestSupport setting(String key, Boolean value) {
        super.setting(key, value);
        return this;
    }

    public TransportClientIngestSupport shards(int value) {
        super.shards(value);
        return this;
    }

    public TransportClientIngestSupport replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public TransportClientIngestSupport newIndex() {
        return newIndex(true);
    }

    public synchronized TransportClientIngestSupport newIndex(boolean ignoreException) {
        if (!enabled) {
            return this;
        }
        super.newIndex(ignoreException);
        return this;
    }

    public TransportClientIngestSupport deleteIndex() {
        return deleteIndex(true);
    }

    public TransportClientIngestSupport deleteIndex(boolean ignoreException) {
        if (!enabled) {
            return this;
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given to create");
            return this;
        }
        try {
            client.admin().indices().delete(new DeleteIndexRequest(getIndex()));
        } catch (Exception e) {
            if (!ignoreException) {
                throw new RuntimeException(e);
            }
        }
        return this;
    }

    @Override
    public TransportClientIngestSupport newType() {
        if (!enabled) {
            return this;
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        if (mapping() == null) {
            mapping(defaultMapping());
        }
        client.admin().indices().putMapping(new PutMappingRequest()
                .indices(new String[]{getIndex()})
                .type(getType())
                .source(mapping()))
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
        if (!enabled) {
            return this;
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        try {
            client.admin().indices().deleteMapping(new DeleteMappingRequest()
                        .indices(new String[]{getIndex()})
                        .type(type));
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

    @Override
    public TransportClientIngestSupport refresh() {
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        client.admin().indices().refresh(new RefreshRequest());
        return this;
    }

    @Override
    public TransportClientIngestSupport createDocument(String index, String type, String id, String source) {
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
            logger.error("bulk add of create failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public TransportClientIngestSupport indexDocument(String index, String type, String id, String source) {
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
            logger.error("bulk add of index failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public TransportClientIngestSupport deleteDocument(String index, String type, String id) {
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
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
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
    public TransportClientIngestSupport waitForHealthyCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        super.waitForHealthyCluster(status, timeout);
        return this;
    }

    public TransportClientIngestSupport numberOfShards(int value) {
        if (!enabled) {
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_shards", value);
        return this;
    }

    public TransportClientIngestSupport numberOfReplicas(int value) {
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        if (getIndex() == null) {
            logger.warn("no index name given");
            return -1;
        }
        super.waitForHealthyCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
        super.update("number_of_replicas", level);
        return super.waitForRecovery();
    }

    @Override
    public TransportClientIngestSupport flush() {
        if (!enabled) {
            return this;
        }
        if (client == null) {
            logger.warn("no client");
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
        if (client == null) {
            logger.warn("no client");
            return;
        }
        try {
            if (ingestProcessor != null) {
                logger.info("closing ingest processor...");
                ingestProcessor.close();
            }
            logger.info("enabling refresh interval...");
            enableRefreshInterval();
            logger.info("shutting down...");
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
