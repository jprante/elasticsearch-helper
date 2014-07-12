package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;
import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.support.client.BaseIngestTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.State;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Ingest client
 */
public class IngestTransportClient extends BaseIngestTransportClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportClient.class.getSimpleName());

    /**
     * The default size of a request
     */
    private int maxActionsPerRequest = 100;

    /**
     * The default number of maximum concurrent requests
     */
    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 2;

    private ByteSizeValue maxVolumePerBulkRequest = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    /**
     * The maximum wait time for responses when shutting down
     */
    private TimeValue maxWaitForResponses = new TimeValue(60, TimeUnit.SECONDS);

    private IngestProcessor ingestProcessor;

    private State state;

    private Throwable throwable;

    private volatile boolean closed = false;

    @Override
    public IngestTransportClient maxActionsPerBulkRequest(int maxBulkActions) {
        this.maxActionsPerRequest = maxBulkActions;
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
    public IngestTransportClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public IngestTransportClient maxRequestWait(TimeValue timeout) {
        this.maxWaitForResponses = timeout;
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
     * Create new client.
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
        IngestProcessor.IngestListener ingestListener = new IngestProcessor.IngestListener() {
            @Override
            public void onRequest(int concurrency, IngestRequest request) {
                int num = request.numberOfActions();
                if (state != null) {
                    state.getSubmitted().inc(num);
                    state.getCurrentIngestNumDocs().inc(num);
                    state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                if (logger.isInfoEnabled()) {
                    logger.info("ingest request [{}] of {} items, {} bytes, {} concurrent requests",
                            request.ingestId(), num, request.estimatedSizeInBytes(), concurrency);
                }
            }

            @Override
            public void onResponse(int concurrency, IngestResponse response) {
                if (state != null) {
                    state.getSucceeded().inc(response.successSize());
                    state.getFailed().inc(response.getFailures().size());
                    state.getTotalIngest().inc(response.tookInMillis());
                }
                if (logger.isInfoEnabled()) {
                    logger.info("ingest response [{}] [succeeded={}] [failed={}] [{}ms] [leader={}] [replica={}]",
                            response.ingestId(),
                            state.getSucceeded().count(),
                            state.getFailed().count(),
                            response.tookInMillis(),
                            response.leaderShardResponse(),
                            response.replicaShardResponses());
                }
                if (!response.getFailures().isEmpty()) {
                    for (IngestActionFailure f : response.getFailures()) {
                        logger.error("ingest [{}] has failures, reason: {}", response.ingestId(), f.message());
                    }
                    closed = true;
                } else {
                    state.getCurrentIngestNumDocs().dec(response.successSize());
                }
            }

            @Override
            public void onFailure(int concurrency, long executionId, Throwable failure) {
                logger.error("ingest [" + executionId + "] failure", failure);
                throwable = failure;
                closed = true;
            }
        };
        this.ingestProcessor = new IngestProcessor(client)
                .maxConcurrentRequests(maxConcurrentBulkRequests)
                .maxActions(maxActionsPerRequest)
                .maxVolumePerRequest(maxVolumePerBulkRequest)
                .flushInterval(flushInterval)
                .maxWaitForResponses(maxWaitForResponses)
                .listener(ingestListener);
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

    public IngestTransportClient shards(int value) {
        super.shards(value);
        return this;
    }

    public IngestTransportClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public IngestTransportClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.newIndex(index);
        return this;
    }

    public IngestTransportClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex(index);
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
    public IngestTransportClient startBulk(String index) throws IOException {
        if (state == null) {
            return this;
        }
        if (!state.isBulk(index)) {
            state.startBulk(index);
            ClientHelper.disableRefresh(client, index);
        }
        return this;
    }

    @Override
    public IngestTransportClient stopBulk(String index) throws IOException {
        if (state == null) {
            return this;
        }
        if (state.isBulk(index)) {
            state.stopBulk(index);
            ClientHelper.enableRefresh(client, index);
            flush(index);
            refresh(index);
        }
        return this;
    }

    @Override
    public IngestTransportClient flush(String index) {
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given");
            return this;
        }
        ClientHelper.flush(client(), index);
        return this;
    }

    @Override
    public IngestTransportClient refresh(String index) {
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given");
            return this;
        }
        ClientHelper.refresh(client(), index);
        return this;
    }

    @Override
    public IngestTransportClient index(String index, String type, String id, String source) {
        return index(new IndexRequest(index).type(type).id(id).create(false).source(source));
    }

    @Override
    public IngestTransportClient index(IndexRequest indexRequest) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        try {
            state.getCurrentIngest().inc();
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            logger.error("add of index request failed: " + e.getMessage(), e);
            throwable = e;
            //closed = true;
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public IngestTransportClient delete(String index, String type, String id) {
        return delete(new DeleteRequest(index).type(type).id(id));
    }

    @Override
    public IngestTransportClient delete(DeleteRequest deleteRequest) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        try {
            state.getCurrentIngest().inc();
            ingestProcessor.add(deleteRequest);
        } catch (Exception e) {
            logger.error("add of delete request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public IngestTransportClient flushIngest() {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        ingestProcessor.flush();
        return this;
    }

    @Override
    public IngestTransportClient waitForResponses(TimeValue maxWaitTime) throws InterruptedException {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        ingestProcessor.waitForResponses(maxWaitTime);
        return this;
    }

    @Override
    public IngestTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
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
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client was closed, possible reason: ", throwable);
            }
            throw new ElasticsearchIllegalStateException("client was closed");
        }
        closed = true;
        if (client == null) {
            logger.warn("no client");
            return;
        }
        try {
            if (ingestProcessor != null) {
                logger.info("closing ingest processor...");
                ingestProcessor.close();
            }
            if (state.indices() != null && !state.indices().isEmpty()) {
                logger.info("stopping ingest mode for indices {}...", state.indices());
                for (String index : ImmutableSet.copyOf(state.indices())) {
                    stopBulk(index);
                }
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
