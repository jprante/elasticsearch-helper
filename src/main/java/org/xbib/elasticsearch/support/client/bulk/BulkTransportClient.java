
package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
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

import org.xbib.elasticsearch.support.client.AbstractIngestTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.ConfigHelper;
import org.xbib.elasticsearch.support.client.State;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  Client using the BulkProcessor of Elasticsearch
 */
public class BulkTransportClient extends AbstractIngestTransportClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportClient.class.getSimpleName());
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

    /**
     * The BulkProcessor
     */
    private BulkProcessor bulkProcessor;

    private State state;

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
    public BulkTransportClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        return this;
    }

    @Override
    public BulkTransportClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public BulkTransportClient maxVolumePerBulkRequest(ByteSizeValue maxVolumePerBulkRequest) {
        this.maxVolumePerBulkRequest = maxVolumePerBulkRequest;
        return this;
    }

    @Override
    public BulkTransportClient maxRequestWait(TimeValue timeout) {
        // ignore, not supported
        return this;
    }

    public BulkTransportClient flushInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public TimeValue flushInterval() {
        return flushInterval;
    }

    public BulkTransportClient newClient(Client client) {
        return this.newClient(findURI());
    }

    /**
     * Create a new client
     *
     * @return this client
     */
    public BulkTransportClient newClient() {
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
    public BulkTransportClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public BulkTransportClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        resetSettings();
        this.state = new State();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = outstandingRequests.getAndIncrement();
                state.getSubmitted().inc(request.numberOfActions());
                state.getCurrentIngestNumDocs().inc(request.numberOfActions());
                state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                            executionId, request.numberOfActions(), request.estimatedSizeInBytes(), l);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                outstandingRequests.decrementAndGet();
                state.getSucceeded().inc(response.getItems().length);
                state.getFailed().inc(request.numberOfActions() - response.getItems().length);
                state.getTotalIngest().inc(response.getTookInMillis());
                if (logger.isDebugEnabled()) {
                    logger.debug("bulk [{}] [{} items] [{}] [{}ms]",
                            executionId,
                            response.getItems().length,
                            response.hasFailures() ? "failure" : "ok",
                            response.getTook().millis());
                }
                if (response.hasFailures()) {
                    // we do not close, maybe mapping failures
                    logger.error("bulk [{}] failure reason: {}",
                            executionId, response.buildFailureMessage());
                } else {
                    state.getCurrentIngestNumDocs().dec(response.getItems().length);
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
                .setBulkActions(maxActionsPerBulkRequest)
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
    public BulkTransportClient setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public BulkTransportClient setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
    }

    public BulkTransportClient setting(String key, String value) {
        configHelper.setting(key, value);
        return this;
    }

    public BulkTransportClient setting(String key, Integer value) {
        configHelper.setting(key, value);
        return this;
    }

    public BulkTransportClient setting(String key, Boolean value) {
        configHelper.setting(key, value);
        return this;
    }

    public BulkTransportClient setting(InputStream in) throws IOException {
        configHelper.setting(in);
        return this;
    }

    public Settings settings() {
        return configHelper.settings();
    }

    public BulkTransportClient mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type,in);
        return this;
    }

    public BulkTransportClient mapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
        return this;
    }

    public Map<String,String> mappings() {
        return configHelper.mappings();
    }

    public BulkTransportClient shards(int value) {
        super.shards(value);
        return this;
    }

    public BulkTransportClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public BulkTransportClient newIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.newIndex();
        return this;
    }

    @Override
    public BulkTransportClient deleteIndex() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex();
        return this;
    }

    @Override
    public BulkTransportClient startBulk() throws IOException {
        if (state.isBulk()) {
            return this;
        }
        state.setBulk(true);
        ClientHelper.startBulk(client, index);
        return this;
    }

    @Override
    public BulkTransportClient stopBulk() throws IOException {
        if (state.isBulk()) {
            ClientHelper.stopBulk(client, index);
        }
        state.setBulk(false);
        return this;
    }

    @Override
    public BulkTransportClient refresh() {
        ClientHelper.refresh(client, index);
        return this;
    }

    @Override
    public BulkTransportClient index(String index, String type, String id, String source) {
        return index(Requests.indexRequest(index).type(type).id(id).create(false).source(source));
    }

    @Override
    public BulkTransportClient index(IndexRequest indexRequest) {
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
    public BulkTransportClient delete(String index, String type, String id) {
        return delete(Requests.deleteRequest(index).type(type).id(id));
    }

    @Override
    public BulkTransportClient delete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    public BulkTransportClient flush() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        // we simply wait long enough for BulkProcessor flush plus one second
        try {
            logger.info("flushing bulk processor (forced wait of {} seconds...)", 2 * flushInterval.seconds());
            Thread.sleep(2 * flushInterval.getMillis());
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        }
        return this;
    }

    @Override
    public BulkTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        ClientHelper.waitForCluster(client, status, timeValue);
        return this;
    }

    @Override
    public int waitForRecovery() throws IOException {
        return ClientHelper.waitForRecovery(client, index);
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return ClientHelper.updateReplicaLevel(client, index, level);
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
            if (bulkProcessor != null) {
                flush();
                bulkProcessor.close();
            }
            if (state.isBulk()) {
                ClientHelper.stopBulk(client, index);
            }
            logger.info("shutting down...");
            super.shutdown();
            logger.info("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public State getState() {
        return state;
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
