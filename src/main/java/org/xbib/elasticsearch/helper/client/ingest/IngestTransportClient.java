package org.xbib.elasticsearch.helper.client.ingest;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;
import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.helper.client.BaseMetricTransportClient;
import org.xbib.elasticsearch.helper.client.ClientHelper;
import org.xbib.elasticsearch.helper.client.Ingest;
import org.xbib.elasticsearch.helper.client.IngestMetric;

import java.io.IOException;
import java.util.Map;

/**
 * Ingest transport client
 */
public class IngestTransportClient extends BaseMetricTransportClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportClient.class.getName());

    private int maxActionsPerRequest = DEFAULT_MAX_ACTIONS_PER_REQUEST;

    private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;

    private ByteSizeValue maxVolumePerRequest = DEFAULT_MAX_VOLUME_PER_REQUEST;

    private TimeValue flushInterval = DEFAULT_FLUSH_INTERVAL;

    private IngestProcessor ingestProcessor;

    private Throwable throwable;

    private volatile boolean closed;

    @Override
    public IngestTransportClient maxActionsPerRequest(int maxActionsPerRequest) {
        this.maxActionsPerRequest = maxActionsPerRequest;
        return this;
    }

    @Override
    public IngestTransportClient maxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    @Override
    public IngestTransportClient maxVolumePerRequest(ByteSizeValue maxVolumePerRequest) {
        this.maxVolumePerRequest = maxVolumePerRequest;
        return this;
    }

    @Override
    public IngestTransportClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public IngestTransportClient init(ElasticsearchClient client, IngestMetric metric) throws IOException {
        return this.init(findSettings(), metric);
    }

    @Override
    public IngestTransportClient init(Map<String, String> settings, IngestMetric metric) throws IOException {
        return this.init(Settings.settingsBuilder().put(settings).build(), metric);
    }

    @Override
    public IngestTransportClient init(Settings settings, final IngestMetric metric) throws IOException {
        super.init(settings, metric);
        resetSettings();
        IngestProcessor.IngestListener ingestListener = new IngestProcessor.IngestListener() {
            @Override
            public void onRequest(int concurrency, IngestRequest request) {
                metric.getCurrentIngest().inc();
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
                metric.getCurrentIngest().dec();
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
                metric.getCurrentIngest().dec();
                logger.error("failure of ingest [" + executionId + "]", failure);
                throwable = failure;
                closed = true;
            }
        };
        this.ingestProcessor = new IngestProcessor(client)
                .maxConcurrentRequests(maxConcurrentRequests)
                .maxActions(maxActionsPerRequest)
                .maxVolumePerRequest(maxVolumePerRequest)
                .flushInterval(flushInterval)
                .listener(ingestListener);
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
    public IngestTransportClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        super.newIndex(index);
        return this;
    }

    @Override
    public IngestTransportClient newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        super.newIndex(index, settings, mappings);
        return this;
    }

    public IngestTransportClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        super.deleteIndex(index);
        return this;
    }


    @Override
    public IngestTransportClient putMapping(String index) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        super.putMapping(index);
        return this;
    }

    @Override
    public IngestTransportClient startBulk(String index, long startRefreshIntervalMillis, long stopRefreshItervalMillis) throws IOException {
        super.startBulk(index, startRefreshIntervalMillis, stopRefreshItervalMillis);
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
                throw new ElasticsearchException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchException("client is closed");
            }
        }
        try {
            metric.getCurrentIngest().inc();
            ingestProcessor.add(new IndexRequest(index).type(type).id(id).source(source));
        } catch (Exception e) {
            logger.error("add of index request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            metric.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public IngestTransportClient bulkIndex(org.elasticsearch.action.index.IndexRequest indexRequest) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchException("client is closed");
            }
        }
        try {
            metric.getCurrentIngest().inc();
            ingestProcessor.add(new IndexRequest(indexRequest));
        } catch (Exception e) {
            logger.error("add of index request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            metric.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public IngestTransportClient delete(String index, String type, String id) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchException("client is closed");
            }
        }
        try {
            metric.getCurrentIngest().inc();
            ingestProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            logger.error("add of delete request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            metric.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public IngestTransportClient bulkDelete(org.elasticsearch.action.delete.DeleteRequest deleteRequest) {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchException("client is closed");
            }
        }
        try {
            metric.getCurrentIngest().inc();
            ingestProcessor.add(new DeleteRequest(deleteRequest));
        } catch (Exception e) {
            logger.error("add of delete request failed: " + e.getMessage(), e);
            throwable = e;
            closed = true;
        } finally {
            metric.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public Ingest update(String index, String type, String id, String source) {
        // we will never implement this!
        throw new UnsupportedOperationException();
    }

    @Override
    public Ingest bulkUpdate(UpdateRequest updateRequest) {
        // we will never implement this!
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestTransportClient flushIngest() {
        if (closed) {
            if (throwable != null) {
                throw new ElasticsearchException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchException("client is closed");
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
                throw new ElasticsearchException("client is closed, possible reason: ", throwable);
            } else {
                throw new ElasticsearchException("client is closed");
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
                throw new ElasticsearchException("client was closed, possible reason: ", throwable);
            }
            throw new ElasticsearchException("client was closed");
        }
        closed = true;
        if (client == null) {
            logger.warn("no client");
            return;
        }
        try {
            if (ingestProcessor != null) {
                logger.debug("closing ingest");
                ingestProcessor.close();
            }
            if (metric != null && metric.indices() != null && !metric.indices().isEmpty()) {
                logger.debug("stopping ingest mode for indices {}...", metric.indices());
                for (String index : ImmutableSet.copyOf(metric.indices())) {
                    stopBulk(index);
                }
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
