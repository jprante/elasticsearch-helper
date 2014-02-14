
package org.xbib.elasticsearch.support.client.ingest.index;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.action.ingest.index.IngestIndexProcessor;
import org.xbib.elasticsearch.action.ingest.index.IngestIndexRequest;
import org.xbib.elasticsearch.support.client.AbstractIngestClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Ingest index client
 */
public class IngestIndexClient extends AbstractIngestClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestIndexClient.class.getName());
    /**
     * The default size of a request
     */
    private int maxBulkActions = 100;

    /**
     * The default number of maximum concurrent requests
     */
    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 4;
    /**
     * The maximum volume
     */
    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    /**
     * The maximum wait time for responses when shutting down
     */
    private TimeValue maxWaitTime = new TimeValue(60, TimeUnit.SECONDS);

    private final MeanMetric totalIngest = new MeanMetric();

    private final CounterMetric totalIngestNumDocs = new CounterMetric();

    private final CounterMetric totalIngestSizeInBytes = new CounterMetric();

    private final CounterMetric currentIngest = new CounterMetric();

    private final CounterMetric currentIngestNumDocs = new CounterMetric();

    /**
     * The processor
     */
    private IngestIndexProcessor ingestProcessor;

    /**
     * The last exception if any
     */
    private Throwable throwable;

    private volatile boolean closed = false;

    @Override
    public IngestIndexClient maxActionsPerBulkRequest(int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    @Override
    public IngestIndexClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public IngestIndexClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    /**
     * Create a new client for this indexer
     *
     * @return this indexer
     */
    @Override
    public IngestIndexClient newClient() {
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
    public IngestIndexClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public IngestIndexClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        resetSettings();
        IngestIndexProcessor.Listener listener = new IngestIndexProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, int concurrency, IngestIndexRequest request) {
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
                    logger.debug("after bulk [{}] [{} items succeeded] [{} items failed] [{}ms] {} outstanding bulk requests",
                        executionId, response.successSize(), response.failure().size(), response.took().millis(), concurrency);
                }
                if (!response.failure().isEmpty()) {
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("after bulk [{}] [{} failure reason: {}", executionId, f.pos(), f.message());
                    }
                } else {
                    currentIngestNumDocs.dec(response.successSize());
                    totalIngestNumDocs.inc(response.successSize());
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, Throwable failure) {
                throwable = failure;
                closed = true;
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

    @Override
    public IngestIndexClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public IngestIndexClient setType(String type) {
        super.setType(type);
        return this;
    }

    public IngestIndexClient setting(String key, String value) {
        super.setting(key, value);
        return this;
    }

    public IngestIndexClient setting(String key, Integer value) {
        super.setting(key, value);
        return this;
    }

    public IngestIndexClient setting(String key, Boolean value) {
        super.setting(key, value);
        return this;
    }

    public IngestIndexClient setting(InputStream in) throws IOException {
        super.setting(in);
        return this;
    }

    public IngestIndexClient shards(int value) {
        super.shards(value);
        return this;
    }

    public IngestIndexClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public IngestIndexClient newIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.newIndex();
        return this;
    }

    public IngestIndexClient deleteIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex();
        return this;
    }

    @Override
    public IngestIndexClient mapping(String type, InputStream in) throws IOException {
        super.mapping(type, in);
        return this;
    }

    @Override
    public IngestIndexClient mapping(String type, String mapping) {
        super.mapping(type, mapping);
        return this;
    }

    @Override
    public IngestIndexClient putMapping(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.putMapping(index);
        return this;
    }

    @Override
    public IngestIndexClient deleteMapping(String index, String type) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteMapping(index,type);
        return this;
    }

    @Override
    public IngestIndexClient startBulk() throws IOException {
        disableRefreshInterval();
        updateReplicaLevel(0);
        return this;
    }

    @Override
    public IngestIndexClient stopBulk() {
        enableRefreshInterval();
        return this;
    }

    @Override
    public IngestIndexClient refresh() {
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
    public IngestIndexClient index(String index, String type, String id, String source) {
        return index(Requests.indexRequest(index).type(type).id(id).create(false).source(source));
    }

    @Override
    public IngestIndexClient index(IndexRequest indexRequest) {
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
    public IngestIndexClient delete(String index, String type, String id) {
        // do nothing
        return this;
    }

    @Override
    public IngestIndexClient delete(DeleteRequest deleteRequest) {
        // do nothing
        return this;
    }

    public IngestIndexClient numberOfShards(int value) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_shards", value);
        return this;
    }

    public IngestIndexClient numberOfReplicas(int value) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    public IngestIndexClient flush() {
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
    public synchronized void shutdown() {
        if (closed) {
            super.shutdown();
            throw new ElasticsearchIllegalStateException("client is closed");
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
