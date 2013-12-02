
package org.xbib.elasticsearch.support.client.delete;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.action.ingest.delete.IngestDeleteProcessor;
import org.xbib.elasticsearch.action.ingest.delete.IngestDeleteRequest;
import org.xbib.elasticsearch.support.client.AbstractIngestClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ingest delete client
 */
public class IngestDeleteClient extends AbstractIngestClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestDeleteClient.class.getSimpleName());

    private int maxBulkActions = 1000;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 8;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private final AtomicLong bulkCounter = new AtomicLong(0L);

    private final AtomicLong volumeCounter = new AtomicLong(0L);

    private volatile boolean enabled = true;

    private IngestDeleteProcessor ingestProcessor;

    private Throwable throwable;

    /**
     * Enable or disable this client
     *
     * @param enabled true for enable, false for disable
     * @return this client
     */
    public IngestDeleteClient enable(boolean enabled) {
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
    public IngestDeleteClient maxBulkActions(int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    @Override
    public IngestDeleteClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public IngestDeleteClient maxVolume(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    /**
     * Create a new client
     *
     * @return this indexer
     */
    @Override
    public IngestDeleteClient newClient() {
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
    public IngestDeleteClient newClient(URI uri) {
        super.newClient(uri);
        IngestDeleteProcessor.Listener listener = new IngestDeleteProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, int concurrency, IngestDeleteRequest request) {
                long v = volumeCounter.addAndGet(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), v, concurrency);
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, IngestResponse response) {
                bulkCounter.incrementAndGet();
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] [{} items succeeded] [{} items failed] [{}ms] {} outstanding bulk requests",
                            executionId, response.successSize(), response.failure().size(), response.took().millis(), concurrency);
                }
                if (!response.failure().isEmpty()) {
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("after bulk [{}] [{} failure reason: {}", executionId, f.id(), f.message());
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, int concurrency, Throwable failure) {
                logger.error("after bulk ["+executionId+"] failure", failure);
                enabled = false;
                throwable = failure;
            }
        };
        this.ingestProcessor = new IngestDeleteProcessor(client, maxConcurrentBulkRequests, maxBulkActions, maxVolume)
                .listener(listener);
        this.enabled = true;
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    /**
     * Initial settings
     * @param uri the cluster name URI
     * @return the initial settings
     */
    @Override
    protected Settings initialSettings(URI uri) {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false) // sniff would join us into any cluster ... bug?
                .build();
    }

    @Override
    public IngestDeleteClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public IngestDeleteClient setType(String type) {
        super.setType(type);
        return this;
    }

    public IngestDeleteClient setting(String key, String value) {
        super.setting(key, value);
        return this;
    }

    public IngestDeleteClient setting(String key, Integer value) {
        super.setting(key, value);
        return this;
    }

    public IngestDeleteClient setting(String key, Boolean value) {
        super.setting(key, value);
        return this;
    }

    public IngestDeleteClient setting(InputStream in) throws IOException {
        super.setting(in);
        return this;
    }

    public IngestDeleteClient shards(int value) {
        super.shards(value);
        return this;
    }

    public IngestDeleteClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public IngestDeleteClient newIndex() {
        if (!enabled) {
            return this;
        }
        super.newIndex();
        return this;
    }

    public IngestDeleteClient deleteIndex() {
        if (!enabled) {
            return this;
        }
        super.deleteIndex();
        return this;
    }

    @Override
    public IngestDeleteClient mapping(String type, InputStream in) throws IOException {
        super.mapping(type, in);
        return this;
    }

    @Override
    public IngestDeleteClient mapping(String type, String mapping) {
        super.mapping(type, mapping);
        return this;
    }

    @Override
    public IngestDeleteClient putMapping(String index) {
        if (!enabled) {
            return this;
        }
        super.putMapping(index);
        return this;
    }

    @Override
    public IngestDeleteClient deleteMapping(String index, String type) {
        if (!enabled) {
            return this;
        }
        super.deleteMapping(index, type);
        return this;
    }

    @Override
    public IngestDeleteClient startBulk() throws IOException {
        disableRefreshInterval();
        updateReplicaLevel(0);
        return this;
    }

    @Override
    public IngestDeleteClient stopBulk() {
        enableRefreshInterval();
        return this;
    }

    @Override
    public IngestDeleteClient refresh() {
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
    public IngestDeleteClient createDocument(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public IngestDeleteClient indexDocument(String index, String type, String id, String source) {

        return this;
    }

    @Override
    public IngestDeleteClient deleteDocument(String index, String type, String id) {
        if (!enabled) {
            return this;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("delete: {}/{}/{}", index, type, id);
        }
        DeleteRequest deleteRequest = Requests.deleteRequest(index).type(type).id(id);
        try {
            ingestProcessor.add(deleteRequest);
        } catch (Exception e) {
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
            throwable = e;
            enabled = false;
        }
        return this;
    }

    @Override
    public IngestDeleteClient waitForCluster() throws IOException {
        super.waitForCluster();
        return this;
    }

    @Override
    public IngestDeleteClient waitForCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        super.waitForCluster(status, timeout);
        return this;
    }

    public IngestDeleteClient numberOfShards(int value) {
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

    public IngestDeleteClient numberOfReplicas(int value) {
        if (getIndex() == null) {
            logger.warn("no index name given");
            return this;
        }
        setting("index.number_of_replicas", value);
        return this;
    }

    @Override
    public IngestDeleteClient flush() {
        if (!enabled) {
            return this;
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
        if (!enabled) {
            super.shutdown();
            return;
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
