
package org.xbib.elasticsearch.support.client.ingest.index;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import org.xbib.elasticsearch.action.ingest.index.IngestIndexProcessor;
import org.xbib.elasticsearch.action.ingest.index.IngestIndexRequest;
import org.xbib.elasticsearch.support.client.BaseIngestTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.State;

/**
 * Ingest index client
 */
public class IngestIndexTransportClient extends BaseIngestTransportClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestIndexTransportClient.class.getName());
    /**
     * The default size of a bulk request
     */
    private int maxBulkActions = 100;

    /**
     * The default number of maximum concurrent requests
     */
    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * The maximum volume of a bulk request
     */
    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    /**
     * The maximum wait time for responses when shutting down
     */
    private TimeValue maxWaitTime = new TimeValue(60, TimeUnit.SECONDS);

    /**
     * The bulk processor
     */
    private IngestIndexProcessor ingestProcessor;

    /**
     * The state
     */
    private State state;

    /**
     * The last exception if any
     */
    private Throwable throwable;

    private volatile boolean closed = false;

    private Set<String> indices = new HashSet();

    @Override
    public IngestIndexTransportClient maxActionsPerBulkRequest(int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    @Override
    public IngestIndexTransportClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public IngestIndexTransportClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    @Override
    public IngestIndexTransportClient maxRequestWait(TimeValue timeout) {
        this.maxWaitTime = timeout;
        return this;
    }

    /**
     * Create a new client for this indexer
     *
     * @return this indexer
     */
    public IngestIndexTransportClient newClient() {
        return this.newClient(findURI());
    }

    public IngestIndexTransportClient newClient(Client client) {
        return this.newClient(findURI());
    }

    /**
     * Create new client with concurrent ingest processor.
     *
     * The URI describes host and port of the node the client should connect to,
     * with the parameter <tt>es.cluster.name</tt> for the cluster name.
     *
     * @param uri the cluster URI
     * @return this indexer
     */
    @Override
    public IngestIndexTransportClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public IngestIndexTransportClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        this.state = new State();
        resetSettings();
        IngestIndexProcessor.Listener listener = new IngestIndexProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, int concurrency, IngestIndexRequest request) {
                if (state != null) {
                    state.getSubmitted().inc(request.numberOfActions());
                    state.getCurrentIngestNumDocs().inc(request.numberOfActions());
                    state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), request.estimatedSizeInBytes(), concurrency);
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
                    logger.debug("after bulk [{}] [{} items succeeded] [{} items failed] [{}ms] {} outstanding bulk requests",
                        executionId, response.successSize(), response.failureSize(), response.took().millis(), concurrency);
                }
                if (!response.hasFailures()) {
                    closed = true;
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("after bulk [{}] [{} failure reason: {}", executionId, f.pos(), f.message());
                    }
                    throwable = new ElasticsearchIllegalStateException("bulk failure [" + executionId + "] "
                        + response.buildFailureMessage());
                } else {
                    if (state != null) {
                        state.getCurrentIngestNumDocs().dec(response.successSize());
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, Throwable failure) {
                closed = true;
                throwable = failure;
                logger.error("after bulk ["+executionId+"] failure", failure);
            }
        };
        this.ingestProcessor = new IngestIndexProcessor(client, maxConcurrentBulkRequests, maxBulkActions, maxVolume, maxWaitTime)
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

    public IngestIndexTransportClient shards(int value) {
        super.shards(value);
        return this;
    }

    public IngestIndexTransportClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public IngestIndexTransportClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.newIndex(index);
        return this;
    }

    public IngestIndexTransportClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex(index);
        return this;
    }

    @Override
    public IngestIndexTransportClient putMapping(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.putMapping(index);
        return this;
    }

    @Override
    public IngestIndexTransportClient deleteMapping(String index, String type) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteMapping(index,type);
        return this;
    }

    @Override
    public IngestIndexTransportClient startBulk(String index) throws IOException {
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
    public IngestIndexTransportClient stopBulk(String index) throws IOException {
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
    public IngestIndexTransportClient refresh(String index) {
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
    public IngestIndexTransportClient index(String index, String type, String id, String source) {
        return index(Requests.indexRequest(index).type(type).id(id).create(false).source(source));
    }

    @Override
    public IngestIndexTransportClient index(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client was closed", throwable);
        }
        try {
            state.getCurrentIngest().inc();
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            closed = true;
            throwable = e;
            logger.error("bulk add of index failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public IngestIndexTransportClient delete(String index, String type, String id) {
        // do nothing
        return this;
    }

    @Override
    public IngestIndexTransportClient delete(DeleteRequest deleteRequest) {
        // do nothing
        return this;
    }

    @Override
    public IngestIndexTransportClient flush() {
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
    public IngestIndexTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
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
                ingestProcessor = null;
            }
            logger.info("stopping bulk mode for indices {}...", indices);
            for (String index : indices) {
                stopBulk(index);
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
