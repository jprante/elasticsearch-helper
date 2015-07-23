package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;
import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.support.client.BaseIngestTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.Metric;

import java.io.IOException;
import java.util.Map;

/**
 * Ingest client
 */
public class IngestTransportClient extends BaseIngestTransportClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportClient.class.getName());

    private int maxActionsPerRequest = DEFAULT_MAX_ACTIONS_PER_REQUEST;
    private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
    private ByteSizeValue maxVolume = DEFAULT_MAX_VOLUME_PER_REQUEST;
    private TimeValue flushInterval = DEFAULT_FLUSH_INTERVAL;

    private IngestProcessor ingestProcessor;

    private Throwable throwable;

    private int currentConcurrency;

    private volatile boolean closed = false;

    @Override
    public IngestTransportClient maxActionsPerRequest(int maxBulkActions) {
        this.maxActionsPerRequest = maxBulkActions;
        return this;
    }

    @Override
    public IngestTransportClient maxConcurrentRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public IngestTransportClient maxVolumePerRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
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
        resetSettings();
        if (metric == null) {
            this.metric = new Metric();
            metric.start();
        }
        IngestProcessor.IngestListener ingestListener = new IngestProcessor.IngestListener() {
            @Override
            public void onRequest(int concurrency, IngestRequest request) {
                setCurrentConcurrency(concurrency);
                int num = request.numberOfActions();
                metric.getSubmitted().inc(num);
                metric.getCurrentIngestNumDocs().inc(num);
                metric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                logger.debug("before ingest [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        request.ingestId(),
                        num,
                        request.estimatedSizeInBytes(),
                        concurrency);
            }

            @Override
            public void onResponse(int concurrency, IngestResponse response) {
                setCurrentConcurrency(concurrency);
                metric.getSucceeded().inc(response.successSize());
                metric.getFailed().inc(response.getFailures().size());
                metric.getTotalIngest().inc(response.tookInMillis());
                logger.debug("after ingest [{}] [succeeded={}] [failed={}] [{}ms] [leader={}] [replica={}] [concurrent requests={}]",
                        response.ingestId(),
                        metric.getSucceeded().count(),
                        metric.getFailed().count(),
                        response.tookInMillis(),
                        response.leaderShardResponse(),
                        response.replicaShardResponses(),
                        concurrency
                );
                if (!response.getFailures().isEmpty()) {
                    for (IngestActionFailure f : response.getFailures()) {
                        logger.error("ingest [{}] has failures, reason: {}", response.ingestId(), f.message());
                    }
                    closed = true;
                } else {
                    metric.getCurrentIngestNumDocs().dec(response.successSize());
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
                .maxConcurrentRequests(maxConcurrentRequests)
                .maxActions(maxActionsPerRequest)
                .maxVolumePerRequest(maxVolume)
                .flushInterval(flushInterval)
                .listener(ingestListener);
        this.closed = false;
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public Metric getMetric() {
        return metric;
    }

    @Override
    public IngestTransportClient setMetric(Metric metric) {
        this.metric = metric;
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
        super.startBulk(index);
        return this;
    }

    @Override
    public IngestTransportClient stopBulk(String index) throws IOException {
        super.stopBulk(index);
        return this;
    }

    @Override
    public IngestTransportClient flushIndex(String index) {
        ClientHelper.flushIndex(client(), index);
        return this;
    }

    @Override
    public IngestTransportClient refreshIndex(String index) {
        ClientHelper.refreshIndex(client(), index);
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
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().inc();
            }
            ingestProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            logger.error("add of index request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().dec();
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
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().inc();
            }
            ingestProcessor.add(new IndexRequest(indexRequest));
        } catch (Exception e) {
            logger.error("add of index request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().dec();
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
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().inc();
            }
            ingestProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            logger.error("add of delete request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().dec();
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
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().inc();
            }
            ingestProcessor.add(new DeleteRequest(deleteRequest));
        } catch (Exception e) {
            logger.error("add of delete request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            if (metric.getCurrentIngest() != null) {
                metric.getCurrentIngest().dec();
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
            if (metric != null && metric.indices() != null && !metric.indices().isEmpty()) {
                logger.info("stopping ingest mode for indices {}...", metric.indices());
                for (String index : ImmutableSet.copyOf(metric.indices())) {
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
