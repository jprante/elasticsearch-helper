
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bulk client support
 */
public class BulkClient extends AbstractIngestClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkClient.class.getSimpleName());
    /**
     * The default size of a bulk request
     */
    private int maxBulkActions = 1000;
    /**
     * The default number of maximum concurrent requests
     */
    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 8;
    /**
     * The maximum volume
     */
    private ByteSizeValue maxVolume;

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    /**
     * The outstanding requests
     */
    private final AtomicLong outstandingRequests = new AtomicLong(0L);
    /**
     * Count the bulk requests
     */
    private final AtomicLong bulkCounter = new AtomicLong(0L);
    /**
     * Count the volume
     */
    private final AtomicLong volumeCounter = new AtomicLong(0L);
    /**
     * Is this client enabled or not?
     */
    private boolean enabled = true;
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


    /**
     * Enable or disable this client
     *
     * @param enabled true for enable, false for disable
     * @return this client
     */
    public BulkClient enable(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Is this client enabled?
     *
     * @return true if enabled, false if disabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public BulkClient maxBulkActions(int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    @Override
    public BulkClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public BulkClient maxVolume(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    public BulkClient flushInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
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
        super.newClient(uri);
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = outstandingRequests.getAndIncrement();
                long v = volumeCounter.addAndGet(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), v, l);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                bulkCounter.incrementAndGet();
                outstandingRequests.decrementAndGet();
                if (logger.isDebugEnabled()) {
                    logger.debug("bulk [{}] [{} items] [{}] [{}ms]",
                        executionId,
                        response.getItems().length,
                        response.hasFailures() ? "failure" : "ok",
                        response.getTook().millis());
                }
                if (response.hasFailures()) {
                    logger.error("bulk [{}] failure reason: {}",
                            executionId, response.buildFailureMessage());
                    enabled = false;
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest requst, Throwable failure) {
                outstandingRequests.decrementAndGet();
                throwable = failure;
                logger.error("bulk ["+executionId+"] error", failure);
                enabled = false;
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxBulkActions-1)  // off-by-one
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval);
        if (maxVolume != null) {
            builder.setBulkSize(maxVolume);
        }
        this.bulkProcessor =  builder.build();
        this.enabled = true;
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    /**
     * Initial settings
     *
     * @param uri the cluster name URI
     * @return the initial settings
     */
    @Override
    protected Settings initialSettings(URI uri) {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false)
                .build();
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
        if (!enabled) {
            return this;
        }
        super.newIndex();
        return this;
    }

    @Override
    public BulkClient deleteIndex() {
        if (!enabled) {
            return this;
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
    public BulkClient createDocument(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("create: {}/{}/{} source = {}", index, type, id, source);
        }
        IndexRequest indexRequest = Requests.indexRequest(index).type(type).id(id).create(true).source(source);
        try {
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            this.throwable = e;
            logger.error("bulk add of create failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public BulkClient indexDocument(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("index: {}/{}/{} source = {}", index, type, id, source);
        }
        IndexRequest indexRequest = Requests.indexRequest(index).type(type).id(id).create(false).source(source);
        try {
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            this.throwable = e;
            logger.error("bulk add of index failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public BulkClient deleteDocument(String index, String type, String id) {
        if (!enabled) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("delete: {}/{}/{} ", index, type, id);
        }
        DeleteRequest deleteRequest = Requests.deleteRequest(index).type(type).id(id);
        try {
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            this.throwable = e;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public BulkClient waitForCluster() throws IOException {
        super.waitForCluster();
        return this;
    }

    @Override
    public BulkClient waitForCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        super.waitForCluster(status, timeout);
        return this;
    }

    public BulkClient numberOfShards(int value) {
        if (!enabled) {
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_shards", value);
        return this;
    }

    public BulkClient numberOfReplicas(int value) {
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    @Override
    public BulkClient flush() {
        if (!enabled) {
            return this;
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        // we simply wait long enough for BulkProcessor flush plus one second
        try {
            logger.info("flushing bulk processor (forced wait of {} seconds...)", flushInterval.seconds() + 1);
            Thread.sleep(flushInterval.getMillis() + 1000L);
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        }
        return this;
    }

    @Override
    public synchronized void shutdown() {
        if (!enabled) {
            super.shutdown();
            return;
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
    public long getVolumeInBytes() {
        return volumeCounter.get();
    }

    public long getBulkCounter() {
        return bulkCounter.get();
    }

    public boolean hasErrors() {
        return throwable != null;
    }

    public Throwable getThrowable() {
        return throwable;
    }
}
