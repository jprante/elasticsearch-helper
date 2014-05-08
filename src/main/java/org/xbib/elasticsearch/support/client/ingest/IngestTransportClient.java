
package org.xbib.elasticsearch.support.client.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.support.client.AbstractIngestTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.State;

/**
 * Ingest client
 */
public class IngestTransportClient extends AbstractIngestTransportClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportClient.class.getSimpleName());

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 4;

    private ByteSizeValue maxVolumePerBulkRequest = new ByteSizeValue(10, ByteSizeUnit.MB);

    private IngestProcessor ingestProcessor;

    private State state;

    private Throwable throwable;

    private volatile boolean closed = false;

    @Override
    public IngestTransportClient maxActionsPerBulkRequest(int maxBulkActions) {
        this.maxActionsPerBulkRequest = maxBulkActions;
        return this;
    }

    @Override
    public IngestTransportClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public IngestTransportClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolumePerBulkRequest = maxVolume;
        return this;
    }

    @Override
    public IngestTransportClient maxRequestWait(TimeValue timeout) {
        this.maxWaitTime = timeout;
        return this;
    }

    /**
     * Create a new client
     *
     * @return this indexer
     */
    public IngestTransportClient newClient() {
        return this.newClient(findURI());
    }

    public IngestTransportClient newClient(Client client) {
        return this.newClient(findURI());
    }

    /**
     * Create new client
     *
     * The URI describes host and port of the node the client should connect to,
     * with the parameter <tt>es.cluster.name</tt> for the cluster name.
     *
     * @param uri the cluster URI
     * @return this indexer
     */
    @Override
    public IngestTransportClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public IngestTransportClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        this.state = new State();
        resetSettings();
        IngestProcessor.Listener listener = new IngestProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, int concurrency, IngestRequest request) {
                int n = request.numberOfActions();
                if (state != null) {
                    state.getSubmitted().inc(n);
                    state.getCurrentIngestNumDocs().inc(n);
                    state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                            executionId, n, request.estimatedSizeInBytes(), concurrency);
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, IngestResponse response) {
                if (state != null) {
                    state.getSucceeded().inc(response.successSize());
                    state.getFailed().inc(response.failureSize());
                    state.getTotalIngest().inc(response.tookInMillis());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] [{} items succeeded] [{} items failed] [{}ms]",
                            executionId, response.successSize(), response.failureSize(), response.tookInMillis());
                }
                if (response.hasFailures()) {
                    closed = true;
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("after bulk [{}] [{}] failure, reason: {}", executionId, f.pos(), f.message());
                    }
                } else {
                    if (state != null) {
                        state.getCurrentIngestNumDocs().dec(response.successSize());
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, Throwable failure) {
                closed = true;
                logger.error("after bulk ["+executionId+"] failure", failure);
                throwable = failure;
            }
        };
        this.ingestProcessor = new IngestProcessor(client, maxConcurrentBulkRequests, maxActionsPerBulkRequest, maxVolumePerBulkRequest, maxWaitTime)
                .listener(listener);
        this.closed = false;
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    public State getState() {
        return state;
    }

    @Override
    public IngestTransportClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public IngestTransportClient setType(String type) {
        super.setType(type);
        return this;
    }

    public IngestTransportClient setting(String key, String value) {
        super.setting(key, value);
        return this;
    }

    public IngestTransportClient setting(String key, Integer value) {
        super.setting(key, value);
        return this;
    }

    public IngestTransportClient setting(String key, Boolean value) {
        super.setting(key, value);
        return this;
    }

    public IngestTransportClient setting(InputStream in) throws IOException {
        super.setting(in);
        return this;
    }

    public IngestTransportClient shards(int value) {
        super.shards(value);
        return this;
    }

    public IngestTransportClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public IngestTransportClient newIndex() {
        if (closed) {
            return this;
        }
        super.newIndex();
        return this;
    }

    public IngestTransportClient deleteIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex();
        return this;
    }

    @Override
    public IngestTransportClient mapping(String type, InputStream in) throws IOException {
        super.mapping(type, in);
        return this;
    }

    @Override
    public IngestTransportClient mapping(String type, String mapping) {
        super.mapping(type, mapping);
        return this;
    }

    @Override
    public IngestTransportClient putMapping(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.putMapping(index);
        return this;
    }

    @Override
    public IngestTransportClient deleteMapping(String index, String type) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteMapping(index, type);
        return this;
    }

    @Override
    public IngestTransportClient startBulk() throws IOException {
        if (state  == null || state.isBulk()) {
            return this;
        }
        state.setBulk(true);
        ClientHelper.startBulk(client, getIndex());
        return this;
    }

    @Override
    public IngestTransportClient stopBulk() throws IOException {
        if (state == null || state.isBulk()) {
            ClientHelper.stopBulk(client, getIndex());
        }
        state.setBulk(false);
        return this;
    }

    @Override
    public IngestTransportClient refresh() {
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
    public IngestTransportClient index(String index, String type, String id, String source) {
        return index(Requests.indexRequest(index).type(type).id(id).create(false).source(source));
    }

    @Override
    public IngestTransportClient index(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (state != null) {
                state.getCurrentIngest().inc();
            }
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            if (state != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public IngestTransportClient delete(String index, String type, String id) {
        return delete(Requests.deleteRequest(index).type(type).id(id));
    }

    @Override
    public IngestTransportClient delete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (state != null) {
                state.getCurrentIngest().inc();
            }
            ingestProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        } finally {
            if (state != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    public IngestTransportClient numberOfShards(int value) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_shards", value);
        return this;
    }

    public IngestTransportClient numberOfReplicas(int value) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    @Override
    public IngestTransportClient flush() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        ingestProcessor.flush();
        return this;
    }

    @Override
    public IngestTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        ClientHelper.waitForCluster(client, status, timeValue);
        return this;
    }

    @Override
    public int waitForRecovery() throws IOException {
        return ClientHelper.waitForRecovery(client, getIndex());
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return ClientHelper.updateReplicaLevel(client, getIndex(), level);
    }

    @Override
    public synchronized void shutdown() {
        if (closed) {
            super.shutdown();
            throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
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
            if (state != null && state.isBulk()) {
                ClientHelper.stopBulk(client, getIndex());
            }
            logger.info("shutting down...");
            super.shutdown();
            logger.info("shutting down completed");
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
