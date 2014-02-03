
package org.xbib.elasticsearch.support.client.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.support.client.AbstractIngestClient;

/**
 * Ingest client
 */
public class IngestClient extends AbstractIngestClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestClient.class.getSimpleName());

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 4;

    private ByteSizeValue maxVolumePerBulkRequest = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue maxWaitTime = new TimeValue(60, TimeUnit.SECONDS);

    private final MeanMetric totalIngest = new MeanMetric();

    private final CounterMetric totalIngestNumDocs = new CounterMetric();

    private final CounterMetric totalIngestSizeInBytes = new CounterMetric();

    private final CounterMetric currentIngest = new CounterMetric();

    private final CounterMetric currentIngestNumDocs = new CounterMetric();

    private IngestProcessor ingestProcessor;

    private Throwable throwable;

    private volatile boolean closed = false;

    @Override
    public IngestClient maxActionsPerBulkRequest(int maxBulkActions) {
        this.maxActionsPerBulkRequest = maxBulkActions;
        return this;
    }

    @Override
    public IngestClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public IngestClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolumePerBulkRequest = maxVolume;
        return this;
    }

    /**
     * Create a new client
     *
     * @return this indexer
     */
    @Override
    public IngestClient newClient() {
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
    public IngestClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public IngestClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        resetSettings();
        IngestProcessor.Listener listener = new IngestProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, int concurrency, IngestRequest request) {
                currentIngestNumDocs.inc(request.numberOfActions());
                totalIngestSizeInBytes.inc(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                            executionId, request.numberOfActions(), request.estimatedSizeInBytes(), concurrency);
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, IngestResponse response) {
                totalIngest.inc(response.tookInMillis());
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] [{} items succeeded] [{} items failed] [{}ms]",
                            executionId, response.successSize(), response.failure().size(), response.tookInMillis());
                }
                if (!response.failure().isEmpty()) {
                    closed = true;
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("after bulk [{}] [{}] failure, reason: {}", executionId, f.pos(), f.message());
                    }
                } else {
                    currentIngestNumDocs.dec(response.successSize());
                    totalIngestNumDocs.inc(response.successSize());
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

    @Override
    public IngestClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public IngestClient setType(String type) {
        super.setType(type);
        return this;
    }

    public IngestClient setting(String key, String value) {
        super.setting(key, value);
        return this;
    }

    public IngestClient setting(String key, Integer value) {
        super.setting(key, value);
        return this;
    }

    public IngestClient setting(String key, Boolean value) {
        super.setting(key, value);
        return this;
    }

    public IngestClient setting(InputStream in) throws IOException {
        super.setting(in);
        return this;
    }

    public IngestClient shards(int value) {
        super.shards(value);
        return this;
    }

    public IngestClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public IngestClient newIndex() {
        if (closed) {
            return this;
        }
        super.newIndex();
        return this;
    }

    public IngestClient deleteIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex();
        return this;
    }

    @Override
    public IngestClient mapping(String type, InputStream in) throws IOException {
        super.mapping(type, in);
        return this;
    }

    @Override
    public IngestClient mapping(String type, String mapping) {
        super.mapping(type, mapping);
        return this;
    }

    @Override
    public IngestClient putMapping(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.putMapping(index);
        return this;
    }

    @Override
    public IngestClient deleteMapping(String index, String type) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteMapping(index, type);
        return this;
    }

    @Override
    public IngestClient startBulk() throws IOException {
        disableRefreshInterval();
        updateReplicaLevel(0);
        return this;
    }

    @Override
    public IngestClient stopBulk() {
        enableRefreshInterval();
        return this;
    }

    @Override
    public IngestClient refresh() {
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
    public IngestClient index(String index, String type, String id, BytesReference source) {
        return index(Requests.indexRequest(index).type(type).id(id).create(false).source(source, false));
    }

    @Override
    public IngestClient index(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            currentIngest.inc();
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index failed: " + e.getMessage(), e);
        } finally {
            currentIngest.dec();
        }
        return this;
    }

    @Override
    public IngestClient delete(String index, String type, String id) {
        return delete(Requests.deleteRequest(index).type(type).id(id));
    }

    @Override
    public IngestClient delete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            currentIngest.inc();
            ingestProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            currentIngest.dec();
        }
        return this;
    }

    public IngestClient numberOfShards(int value) {
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

    public IngestClient numberOfReplicas(int value) {
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    public IngestClient flush() {
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
    public long getTotalSizeInBytes() {
        return totalIngestSizeInBytes.count();
    }

    @Override
    public long getTotalBulkRequestTime() {
        return totalIngest.sum();
    }

    @Override
    public long getTotalBulkRequests() {
        return totalIngest.count();
    }

    @Override
    public long getTotalDocuments() {
        return totalIngestNumDocs.count();
    }

    @Override
    public boolean hasErrors() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

}
