package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
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
import java.util.Map;

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

    private IngestProcessor ingestProcessor;

    private State state = new State();

    private Throwable throwable;

    private int currentConcurrency;

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

    public void setCurrentConcurrency(int concurrency) {
        this.currentConcurrency = concurrency;
    }

    @Override
    public IngestTransportClient init(Client client) throws IOException {
        return this.init(findSettings());
    }

    @Override
    public IngestTransportClient init(Map<String,String> settings) throws IOException {
        return this.init(ImmutableSettings.settingsBuilder().put(settings).build());
    }

    @Override
    public IngestTransportClient init(Settings settings) throws IOException {
        super.init(settings);
        this.state = new State();
        resetSettings();
        IngestProcessor.IngestListener ingestListener = new IngestProcessor.IngestListener() {
            @Override
            public void onRequest(int concurrency, IngestRequest request) {
                setCurrentConcurrency(concurrency);
                int num = request.numberOfActions();
                if (state != null) {
                    state.getSubmitted().inc(num);
                    state.getCurrentIngestNumDocs().inc(num);
                    state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                if (logger.isInfoEnabled()) {
                    logger.info("before ingest [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                            request.ingestId(),
                            num,
                            request.estimatedSizeInBytes(),
                            concurrency);
                }
            }

            @Override
            public void onResponse(int concurrency, IngestResponse response) {
                setCurrentConcurrency(concurrency);
                if (state != null) {
                    state.getSucceeded().inc(response.successSize());
                    state.getFailed().inc(response.getFailures().size());
                    state.getTotalIngest().inc(response.tookInMillis());
                }
                if (logger.isInfoEnabled()) {
                    logger.info("after ingest [{}] [succeeded={}] [failed={}] [{}ms] [leader={}] [replica={}] [concurrent requests={}]",
                            response.ingestId(),
                            state.getSucceeded().count(),
                            state.getFailed().count(),
                            response.tookInMillis(),
                            response.leaderShardResponse(),
                            response.replicaShardResponses(),
                            concurrency
                    );
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

    @Override
    public IngestTransportClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.newIndex(index);
        return this;
    }

    @Override
    public IngestTransportClient newIndex(String index, Settings settings, Map<String,String> mappings) {
        super.newIndex(index, settings, mappings);
        return this;
    }

    @Override
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
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        try {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().inc();
            }
            ingestProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            logger.error("add of index request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public IngestTransportClient bulkIndex(org.elasticsearch.action.index.IndexRequest indexRequest) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        try {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().inc();
            }
            ingestProcessor.add(new IndexRequest(indexRequest));
        } catch (Exception e) {
            logger.error("add of index request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public IngestTransportClient delete(String index, String type, String id) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        try {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().inc();
            }
            ingestProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            logger.error("add of delete request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public IngestTransportClient bulkDelete(org.elasticsearch.action.delete.DeleteRequest deleteRequest) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchIllegalStateException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchIllegalStateException("client is closed");
            }
        }
        try {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().inc();
            }
            ingestProcessor.add(new DeleteRequest(deleteRequest));
        } catch (Exception e) {
            logger.error("add of delete request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (state.getCurrentIngest() != null) {
                state.getCurrentIngest().dec();
            }
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
        if (ingestProcessor != null) {
            ingestProcessor.flush();
        }
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
        if (ingestProcessor != null) {
            ingestProcessor.waitForResponses(maxWaitTime);
        }
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
                logger.info("closing ingest with {} current concurrent requests ", currentConcurrency);
                ingestProcessor.close();
            }
            if (state != null && state.indices() != null && !state.indices().isEmpty()) {
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
