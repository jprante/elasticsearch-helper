
package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.AbstractIngestClient;
import org.xbib.elasticsearch.support.config.ConfigHelper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  Client using the BulkProcessor of ES
 */
public class BulkClient extends AbstractIngestClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkClient.class.getSimpleName());
    /**
     * The default size of a bulk request
     */
    private int maxActionsPerBulkRequest = 100;
    /**
     * The default number of maximum concurrent requests
     */
    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 4;
    /**
     * The maximum volume
     */
    private ByteSizeValue maxVolumePerBulkRequest = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    /**
     * The outstanding requests
     */
    private final AtomicLong outstandingRequests = new AtomicLong(0L);

    private final MeanMetric totalIngest = new MeanMetric();

    private final CounterMetric totalIngestNumDocs = new CounterMetric();

    private final CounterMetric totalIngestSizeInBytes = new CounterMetric();

    private final CounterMetric currentIngest = new CounterMetric();

    private final CounterMetric currentIngestNumDocs = new CounterMetric();
    /**
     * The BulkProcessor
     */
    private BulkProcessor bulkProcessor;
    /**
     * The default index
     */
    private String index;
    /**
     * The default type
     */
    private String type;

    private Throwable throwable;

    private ConfigHelper configHelper = new ConfigHelper();

    private boolean closed = false;

    @Override
    public BulkClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        return this;
    }

    @Override
    public BulkClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public BulkClient maxVolumePerBulkRequest(ByteSizeValue maxVolumePerBulkRequest) {
        this.maxVolumePerBulkRequest = maxVolumePerBulkRequest;
        return this;
    }

    public BulkClient flushInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public TimeValue flushInterval() {
        return flushInterval;
    }

    /**
     * Create a new client
     *
     * @return this client
     */
    @Override
    public BulkClient newClient() {
        return this.newClient(findURI());
    }

    /**
     * Create new client
     * The URI describes host and port of the node the client should connect to,
     * with the parameter <tt>es.cluster.name</tt> for the cluster name.
     *
     * @param uri the cluster URI
     * @return this client
     */
    @Override
    public BulkClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public BulkClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        resetSettings();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = outstandingRequests.getAndIncrement();
                currentIngestNumDocs.inc(request.numberOfActions());
                totalIngestSizeInBytes.inc(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                            executionId, request.numberOfActions(), request.estimatedSizeInBytes(), l);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                outstandingRequests.decrementAndGet();
                totalIngest.inc(response.getTookInMillis());
                if (logger.isDebugEnabled()) {
                    logger.debug("bulk [{}] [{} items] [{}] [{}ms]",
                            executionId,
                            response.items().length,
                            response.hasFailures() ? "failure" : "ok",
                            response.getTook().millis());
                }
                if (response.hasFailures()) {
                    closed = true;
                    logger.error("bulk [{}] failure reason: {}",
                            executionId, response.buildFailureMessage());
                } else {
                    currentIngestNumDocs.dec(response.items().length);
                    totalIngestNumDocs.inc(response.items().length);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest requst, Throwable failure) {
                outstandingRequests.decrementAndGet();
                throwable = failure;
                closed = true;
                logger.error("bulk ["+executionId+"] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxActionsPerBulkRequest-1)  // off-by-one
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval);
        if (maxVolumePerBulkRequest != null) {
            builder.setBulkSize(maxVolumePerBulkRequest);
        }
        this.bulkProcessor =  builder.build();
        this.closed = false;
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public BulkClient setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public BulkClient setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
    }

    public BulkClient setting(String key, String value) {
        configHelper.setting(key, value);
        return this;
    }

    public BulkClient setting(String key, Integer value) {
        configHelper.setting(key, value);
        return this;
    }

    public BulkClient setting(String key, Boolean value) {
        configHelper.setting(key, value);
        return this;
    }

    public BulkClient setting(InputStream in) throws IOException {
        configHelper.setting(in);
        return this;
    }

    public Settings settings() {
        return configHelper.settings();
    }

    public BulkClient mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type,in);
        return this;
    }

    public BulkClient mapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
        return this;
    }

    public Map<String,String> mappings() {
        return configHelper.mappings();
    }

    public BulkClient shards(int value) {
        super.shards(value);
        return this;
    }

    public BulkClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public BulkClient newIndex() {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        super.newIndex();
        return this;
    }

    @Override
    public BulkClient deleteIndex() {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        super.deleteIndex();
        return this;
    }

    @Override
    public BulkClient startBulk() throws IOException {
        disableRefreshInterval();
        updateReplicaLevel(0);
        return this;
    }

    @Override
    public BulkClient stopBulk() throws IOException {
        enableRefreshInterval();
        return this;
    }

    @Override
    public BulkClient refresh() {
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
    public BulkClient index(String index, String type, String id, BytesReference source) {
        return index(Requests.indexRequest(index).type(type).id(id).create(false).source(source, false));
    }

    @Override
    public BulkClient index(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        try {
            currentIngest.inc();
            bulkProcessor.add(indexRequest);
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
    public BulkClient delete(String index, String type, String id) {
        return delete(Requests.deleteRequest(index).type(type).id(id));
    }

    @Override
    public BulkClient delete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        try {
            currentIngest.inc();
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            currentIngest.dec();
        }
        return this;
    }

    public BulkClient numberOfShards(int value) {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_shards", value);
        return this;
    }

    public BulkClient numberOfReplicas(int value) {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    public BulkClient flush() {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        // we simply wait long enough for BulkProcessor flush plus one second
        try {
            logger.info("flushing bulk processor (forced wait of {} seconds...)", flushInterval.seconds());
            Thread.sleep(flushInterval.getMillis());
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        }
        return this;
    }

    @Override
    public synchronized void shutdown() {
        if (closed) {
            super.shutdown();
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return;
        }
        try {
            if (bulkProcessor != null) {
                flush();
                bulkProcessor.close();
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

    public boolean hasErrors() {
        return throwable != null;
    }

    public Throwable getThrowable() {
        return throwable;
    }
}
