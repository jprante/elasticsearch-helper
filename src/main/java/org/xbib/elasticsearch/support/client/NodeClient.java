
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node client support
 */
public class NodeClient implements DocumentIngest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(NodeClient.class.getSimpleName());

    private int maxBulkActions = 1000;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 8;

    private ByteSizeValue maxVolume;

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    private Client client;

    private String index;

    private String type;

    private ConfigHelper configHelper = new ConfigHelper();

    private final AtomicLong outstandingBulkRequests = new AtomicLong(0L);

    private final AtomicLong bulkCounter = new AtomicLong(0L);

    private final AtomicLong volumeCounter = new AtomicLong(0L);

    private boolean enabled = true;

    private BulkProcessor bulkProcessor;

    private Throwable throwable;

    public NodeClient maxBulkActions(int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    public NodeClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    public NodeClient maxVolume(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    public NodeClient flushInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public NodeClient newClient(Client client) {
        this.client = client;
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = outstandingBulkRequests.getAndIncrement();
                long v = volumeCounter.addAndGet(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), v, l);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                bulkCounter.incrementAndGet();
                outstandingBulkRequests.decrementAndGet();
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] success [{} items] [{}ms]",
                            executionId, response.getItems().length, response.getTook().millis());
                }
                if (response.hasFailures()) {
                    logger.error("after bulk [{}] failure reason: {}",
                            executionId, response.buildFailureMessage());
                    enabled = false;
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                outstandingBulkRequests.decrementAndGet();
                logger.error("after bulk ["+executionId+"] error", failure);
                throwable = failure;
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
        try {
            waitForHealthyCluster();
            this.enabled = true;
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            this.enabled = false;
        }
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public NodeClient setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public NodeClient setType(String type) {
        this.type = type;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public TimeValue getFlushInterval() {
        return flushInterval;
    }

    @Override
    public NodeClient createDocument(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        IndexRequest indexRequest = Requests.indexRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType()).id(id).create(true).source(source);
        try {
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            logger.error("bulk add of create failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public NodeClient indexDocument(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        IndexRequest indexRequest = Requests.indexRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType()).id(id).create(false).source(source);
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
    public NodeClient deleteDocument(String index, String type, String id) {
        if (!enabled) {
            return this;
        }
        DeleteRequest deleteRequest = Requests.deleteRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType()).id(id);
        try {
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public NodeClient flush() {
        if (!enabled) {
            return this;
        }
        // we simply wait long enough for BulkProcessor flush
        try {
            Thread.sleep(flushInterval.getMillis() + 1000L);
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        }
        return this;
    }

    @Override
    public void shutdown() {
        flush();
        bulkProcessor.close();
        client.close();
    }

    public NodeClient waitForHealthyCluster() throws IOException {
        return waitForHealthyCluster(ClusterHealthStatus.YELLOW, "30s");
    }

    public NodeClient waitForHealthyCluster(ClusterHealthStatus status, String timeout) throws IOException {
        try {
            logger.info("waiting for cluster health {}...", status.name());
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout).execute().actionGet();
            if (healthResponse.isTimedOut()) {
                throw new IOException("cluster not healthy, cowardly refusing to continue with operations");
            }
        } catch (ElasticSearchTimeoutException e) {
            throw new IOException("cluster not healthy, cowardly refusing to continue with operations");
        }
        return this;
    }

    public NodeClient setting(String key, String value) {
        configHelper.setting(key, value);
        return this;
    }

    public NodeClient setting(String key, Integer value) {
        configHelper.setting(key, value);
        return this;
    }

    public NodeClient setting(String key, Boolean value) {
        configHelper.setting(key, value);
        return this;
    }

    public NodeClient setting(InputStream in) throws IOException {
        configHelper.setting(in);
        return this;
    }

    public Settings settings() {
        return configHelper.settings();
    }

    public NodeClient mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type,in);
        return this;
    }

    public NodeClient mapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
        return this;
    }

    public Map<String,String> mappings() {
        return configHelper.mappings();
    }

    public NodeClient putMapping(String index) {
        configHelper.putMapping(client, index);
        return this;
    }

    public NodeClient deleteMapping(String index, String type) {
        configHelper.deleteMapping(client, index, type);
        return this;
    }

    public NodeClient newIndex() {
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequest request = new CreateIndexRequest(getIndex());
        if (settings() != null) {
            request.settings(settings());
        }
        if (mappings() != null) {
            for (Map.Entry<String,String> me : mappings().entrySet()) {
                request.mapping(me.getKey(), me.getValue());
            }
        }
        logger.info("creating index {} with settings = {}, mappings = {}",
                getIndex(), settings(), mappings());
        try {
            client.admin().indices().create(request).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    public NodeClient deleteIndex() {
        if (client == null) {
            logger.warn("no client for delete index");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        try {
            client.admin().indices().delete(new DeleteIndexRequest(getIndex())).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

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
