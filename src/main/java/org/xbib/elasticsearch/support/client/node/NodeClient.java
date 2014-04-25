
package org.xbib.elasticsearch.support.client.node;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.ElasticsearchIllegalStateException;
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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.ConfigHelper;
import org.xbib.elasticsearch.support.client.State;

/**
 * Node client support
 */
public class NodeClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(NodeClient.class.getSimpleName());

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 4;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    private final ConfigHelper configHelper = new ConfigHelper();

    private final AtomicLong outstandingBulkRequests = new AtomicLong(0L);

    private Client client;

    private BulkProcessor bulkProcessor;

    private String index;

    private String type;

    private State state;

    private boolean closed = false;

    private Throwable throwable;

    @Override
    public NodeClient shards(int shards) {
        configHelper.setting("index.number_of_shards", shards);
        return this;
    }

    @Override
    public NodeClient replica(int replica) {
        configHelper.setting("index.number_of_replica", replica);
        return this;
    }

    @Override
    public NodeClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        return this;
    }

    @Override
    public NodeClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public NodeClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    public NodeClient maxRequestWait(TimeValue timeValue) {
        // ignore, not implemented
        return this;
    }

    public NodeClient flushInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public NodeClient newClient(URI uri) {
        // no-op
        return this;
    }

    public NodeClient newClient(Client client) {
        this.client = client;
        this.state = new State();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = outstandingBulkRequests.getAndIncrement();
                state.getCurrentIngestNumDocs().inc(request.numberOfActions());
                state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), request.estimatedSizeInBytes(), l);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                outstandingBulkRequests.decrementAndGet();
                state.getTotalIngest().inc(response.getTookInMillis());
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] success [{} items] [{}ms]",
                            executionId, response.getItems().length, response.getTook().millis());
                }
                if (response.hasFailures()) {
                    closed = true;
                    logger.error("after bulk [{}] failure reason: {}",
                            executionId, response.buildFailureMessage());
                } else {
                    state.getCurrentIngestNumDocs().dec(response.getItems().length);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                outstandingBulkRequests.decrementAndGet();
                throwable = failure;
                closed = true;
                logger.error("after bulk ["+executionId+"] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxActionsPerBulkRequest)  // off-by-one
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval);
        if (maxVolume != null) {
            builder.setBulkSize(maxVolume);
        }
        this.bulkProcessor =  builder.build();
        try {
            waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
            closed = false;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            closed = true;
        }
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public State getState() {
        return state;
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

    @Override
    public NodeClient index(String index, String type, String id, String source) {
        return index(Requests.indexRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType())
                .id(id)
                .create(false)
                .source(source));
    }

    @Override
    public NodeClient index(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public NodeClient delete(String index, String type, String id) {
        return delete(Requests.deleteRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType())
                .id(id));
    }

    @Override
    public NodeClient delete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public NodeClient flush() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        // we simply wait long enough for BulkProcessor flush
        try {
            Thread.sleep(2 * flushInterval.getMillis());
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        }
        return this;
    }

    @Override
    public NodeClient startBulk() throws IOException {
        if (state.isBulk()) {
            return this;
        }
        state.setBulk(true);
        ClientHelper.startBulk(client, index);
        return this;
    }

    @Override
    public NodeClient stopBulk() throws IOException {
        if (state.isBulk()) {
            state.setBulk(false);
            ClientHelper.stopBulk(client, index);
        }
        return this;
    }

    @Override
    public NodeClient refresh() {
        ClientHelper.refresh(client, index);
        return this;
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return ClientHelper.updateReplicaLevel(client, index, level);
    }


    @Override
    public NodeClient waitForCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        ClientHelper.waitForCluster(client, status, timeout);
        return this;
    }

    @Override
    public int waitForRecovery() throws IOException {
        return ClientHelper.waitForRecovery(client, index);
    }

    @Override
    public void shutdown() {
        try {
            flush();
            bulkProcessor.close();
            stopBulk();
            client.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    @Override
    public NodeClient resetSettings() {
        configHelper.reset();
        return this;
    }

    @Override
    public NodeClient setting(String key, String value) {
        configHelper.setting(key, value);
        return this;
    }

    @Override
    public NodeClient setting(String key, Integer value) {
        configHelper.setting(key, value);
        return this;
    }

    @Override
    public NodeClient setting(String key, Boolean value) {
        configHelper.setting(key, value);
        return this;
    }

    @Override
    public NodeClient setting(InputStream in) throws IOException {
        configHelper.setting(in);
        return this;
    }

    @Override
    public Settings settings() {
        return configHelper.settings();
    }

    @Override
    public NodeClient mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type,in);
        return this;
    }

    @Override
    public NodeClient mapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
        return this;
    }

    @Override
    public NodeClient newIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
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
                getIndex(), settings().getAsMap(), mappings());
        try {
            client.admin().indices().create(request).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public NodeClient deleteIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
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

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public Map<String,String> mappings() {
        return configHelper.mappings();
    }

}
