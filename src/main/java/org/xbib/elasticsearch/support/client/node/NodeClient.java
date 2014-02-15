
package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.xbib.elasticsearch.support.client.bulk.BulkProcessor;
import org.xbib.elasticsearch.support.config.ConfigHelper;
import org.xbib.elasticsearch.support.client.Feeder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node client support
 */
public class NodeClient implements Feeder {

    private final static ESLogger logger = ESLoggerFactory.getLogger(NodeClient.class.getName());

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 4;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    private Client client;

    private String index;

    private String type;

    private ConfigHelper configHelper = new ConfigHelper();

    private final AtomicLong outstandingBulkRequests = new AtomicLong(0L);

    private final MeanMetric totalIngest = new MeanMetric();

    private final CounterMetric totalIngestNumDocs = new CounterMetric();

    private final CounterMetric totalIngestSizeInBytes = new CounterMetric();

    private final CounterMetric currentIngest = new CounterMetric();

    private final CounterMetric currentIngestNumDocs = new CounterMetric();

    private boolean closed = false;

    private BulkProcessor bulkProcessor;

    private Throwable throwable;

    public NodeClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
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
                currentIngestNumDocs.inc(request.numberOfActions());
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), l);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                outstandingBulkRequests.decrementAndGet();
                totalIngest.inc(response.getTookInMillis());
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] success [{} items] [{}ms]",
                            executionId, response.items().length, response.getTook().millis());
                }
                if (response.hasFailures()) {
                    closed = true;
                    logger.error("after bulk [{}] failure reason: {}",
                            executionId, response.buildFailureMessage());
                } else {
                    currentIngestNumDocs.dec(response.items().length);
                    totalIngestNumDocs.inc(response.items().length);
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
                .setBulkActions(maxActionsPerBulkRequest-1)  // off-by-one
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval);
        if (maxVolume != null) {
            builder.setBulkSize(maxVolume);
        }
        this.bulkProcessor =  builder.build();
        try {
            waitForHealthyCluster();
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

    public NodeClient setIndex(String index) {
        this.index = index;
        return this;
    }

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
    public NodeClient delete(String index, String type, String id) {
        return delete(Requests.deleteRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType())
                .id(id));
    }

    @Override
    public NodeClient delete(DeleteRequest deleteRequest) {
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

    public NodeClient flush() {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
        }
        // we simply wait long enough for BulkProcessor flush
        try {
            Thread.sleep(2 * flushInterval.getMillis());
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        }
        return this;
    }

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
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
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
                getIndex(), settings(), mappings());
        try {
            client.admin().indices().create(request).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    public NodeClient deleteIndex() {
        if (closed) {
            throw new ElasticSearchIllegalStateException("client is closed");
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

    public long getTotalSizeInBytes() {
        return totalIngestSizeInBytes.count();
    }

    public long getTotalBulkRequestTime() {
        return totalIngest.sum();
    }

    public long getTotalBulkRequests() {
        return totalIngest.count();
    }

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
