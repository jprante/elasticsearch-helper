
package org.xbib.elasticsearch.support.client.ingest.delete;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.action.ingest.delete.IngestDeleteProcessor;
import org.xbib.elasticsearch.action.ingest.delete.IngestDeleteRequest;
import org.xbib.elasticsearch.support.client.BaseIngestTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.State;

/**
 * Ingest delete client
 */
public class IngestDeleteTransportClient extends BaseIngestTransportClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestDeleteTransportClient.class.getName());

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 2;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private IngestDeleteProcessor ingestProcessor;

    private State state;

    private Throwable throwable;

    private volatile boolean closed = false;

    private Set<String> indices = new HashSet();

    @Override
    public IngestDeleteTransportClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        return this;
    }

    @Override
    public IngestDeleteTransportClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public IngestDeleteTransportClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    @Override
    public IngestDeleteTransportClient maxRequestWait(TimeValue timeout) {
        this.maxWaitTime = timeout;
        return this;
    }

    /**
     * Create a new client
     *
     * @return this indexer
     */
    public IngestDeleteTransportClient newClient() {
        return this.newClient(findURI());
    }

    public IngestDeleteTransportClient newClient(Client client) {
        return this.newClient(findURI());
    }

    /**
     * Create new client
     * The URI describes host and port of the node the client should connect to,
     * with the parameter <tt>es.cluster.name</tt> for the cluster name.
     *
     * @param uri the cluster URI
     * @return this indexer
     */
    @Override
    public IngestDeleteTransportClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public IngestDeleteTransportClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        this.state = new State();
        resetSettings();
        IngestDeleteProcessor.Listener listener = new IngestDeleteProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, int concurrency, IngestDeleteRequest request) {
                state.getSubmitted().inc(request.numberOfActions());
                state.getCurrentIngestNumDocs().inc(request.numberOfActions());
                state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), request.estimatedSizeInBytes(), concurrency);
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, IngestResponse response) {
                state.getSucceeded().inc(response.successSize());
                state.getFailed().inc(response.failureSize());
                state.getTotalIngest().inc(response.tookInMillis());
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] [{} items succeeded] [{} items failed] [{}ms] {} outstanding bulk requests",
                            executionId, response.successSize(), response.failureSize(), response.took().millis(), concurrency);
                }
                if (!response.failure().isEmpty()) {
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("after bulk [{}] [{} failure reason: {}", executionId, f.pos(), f.message());
                    }
                } else {
                    state.getCurrentIngestNumDocs().dec(response.successSize());
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, Throwable failure) {
                logger.error("after bulk ["+executionId+"] failure", failure);
                throwable = failure;
                closed = true;
            }
        };
        this.ingestProcessor = new IngestDeleteProcessor(client,
                maxConcurrentBulkRequests, maxActionsPerBulkRequest, maxVolume, maxWaitTime)
                .listener(listener);
        this.closed = false;
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public State getState() {
        return state;
    }


    public IngestDeleteTransportClient shards(int value) {
        super.shards(value);
        return this;
    }

    public IngestDeleteTransportClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public IngestDeleteTransportClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.newIndex(index);
        return this;
    }

    public IngestDeleteTransportClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex(index);
        return this;
    }

    @Override
    public IngestDeleteTransportClient putMapping(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.putMapping(index);
        return this;
    }

    @Override
    public IngestDeleteTransportClient deleteMapping(String index, String type) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteMapping(index, type);
        return this;
    }

    @Override
    public IngestDeleteTransportClient startBulk(String index) throws IOException {
        if (state == null) {
            return this;
        }
        if (!state.isBulk()) {
            state.setBulk(true);
            ClientHelper.startBulk(client, index);
        }
        indices.add(index);
        return this;
    }

    @Override
    public IngestDeleteTransportClient stopBulk(String index) throws IOException {
        if (state == null) {
            return this;
        }
        if (state.isBulk()) {
            state.setBulk(false);
            ClientHelper.stopBulk(client, index);
        }
        indices.remove(index);
        return this;
    }


    @Override
    public IngestDeleteTransportClient refresh(String index) {
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given");
            return this;
        }
        client.admin().indices().refresh(new RefreshRequest());
        return this;
    }

    @Override
    public IngestDeleteTransportClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public IngestDeleteTransportClient index(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public IngestDeleteTransportClient delete(String index, String type, String id) {
        return delete(Requests.deleteRequest(index).type(type).id(id));
    }

    @Override
    public IngestDeleteTransportClient delete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            ingestProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    public IngestDeleteTransportClient flush() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        ingestProcessor.flush();
        return this;
    }

    @Override
    public IngestDeleteTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        ClientHelper.waitForCluster(client, status, timeValue);
        return this;
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        return ClientHelper.waitForRecovery(client, index);
    }

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        return ClientHelper.updateReplicaLevel(client, index, level);
    }

    @Override
    public synchronized void shutdown() {
        if (closed) {
            super.shutdown();
            throw new ElasticsearchIllegalStateException("client was closed, possible reason: ", throwable);
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
            if (state.isBulk()) {

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
