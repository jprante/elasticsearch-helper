package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.ConfigHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.Metric;
import org.xbib.elasticsearch.support.client.BulkProcessorHelper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Node client support
 */
public class BulkNodeClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkNodeClient.class.getSimpleName());

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 2;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    private final ConfigHelper configHelper = new ConfigHelper();

    private Client client;

    private BulkProcessor bulkProcessor;

    private Metric metric;

    private Throwable throwable;

    private boolean closed = false;

   /* @Override
    public BulkNodeClient shards(int shards) {
        configHelper.setting("index.number_of_shards", shards);
        return this;
    }

    @Override
    public BulkNodeClient replica(int replica) {
        configHelper.setting("index.number_of_replica", replica);
        return this;
    }*/

    @Override
    public BulkNodeClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        return this;
    }

    @Override
    public BulkNodeClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public BulkNodeClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    @Override
    public BulkNodeClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public BulkNodeClient newClient(Settings settings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkNodeClient newClient(Map<String,String> settings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkNodeClient newClient(Client client) {
        this.client = client;
        if (metric == null) {
            this.metric = new Metric();
            metric.start();
        }
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                metric.getCurrentIngest().inc();
                long l = metric.getCurrentIngest().count();
                int n = request.numberOfActions();
                metric.getSubmitted().inc(n);
                metric.getCurrentIngestNumDocs().inc(n);
                metric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                metric.getCurrentIngest().dec();
                long l = metric.getCurrentIngest().count();
                metric.getSucceeded().inc(response.getItems().length);
                metric.getFailed().inc(0);
                metric.getTotalIngest().inc(response.getTookInMillis());
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (itemResponse.isFailed()) {
                        n++;
                        metric.getSucceeded().dec(1);
                        metric.getFailed().inc(1);
                    }
                }
                logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] {} concurrent requests",
                            executionId,
                            metric.getSucceeded().count(),
                            metric.getFailed().count(),
                            response.getTook().millis(),
                            l);
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    metric.getCurrentIngestNumDocs().dec(response.getItems().length);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                metric.getCurrentIngest().dec();
                throwable = failure;
                closed = true;
                logger.error("after bulk [" + executionId + "] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxActionsPerBulkRequest)  // off-by-one
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval);
        if (maxVolume != null) {
            builder.setBulkSize(maxVolume);
        }
        this.bulkProcessor = builder.build();
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
    public BulkNodeClient setMetric(Metric metric) {
        this.metric = metric;
        return this;
    }

    @Override
    public Metric getMetric() {
        return metric;
    }

    @Override
    public BulkNodeClient putMapping(String index) {
        if (client == null) {
            logger.warn("no client for put mapping");
            return this;
        }
        configHelper.putMapping(client, index);
        return this;
    }

    @Override
    public BulkNodeClient deleteMapping(String index, String type) {
        if (client == null) {
            logger.warn("no client for delete mapping");
            return this;
        }
        configHelper.deleteMapping(client, index, type);
        return this;
    }

    @Override
    public BulkNodeClient index(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient delete(String index, String type, String id) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient update(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc();
            bulkProcessor.add(new UpdateRequest().index(index).type(type).id(id).upsert(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        } finally {
            metric.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkUpdate(UpdateRequest updateRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            metric.getCurrentIngest().inc();
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        } finally {
            metric.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public BulkNodeClient flushIngest() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        logger.debug("flushing bulk processor");
        BulkProcessorHelper.flush(bulkProcessor);
        return this;
    }

    @Override
    public BulkNodeClient waitForResponses(TimeValue maxWaitTime) throws InterruptedException {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        BulkProcessorHelper.waitFor(bulkProcessor, maxWaitTime);
        return this;
    }

    @Override
    public BulkNodeClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) throws IOException {
        if (metric == null) {
            return this;
        }
        if (!metric.isBulk(index)) {
            metric.setupBulk(index, startRefreshInterval, stopRefreshIterval);
            ClientHelper.updateIndexSetting(client, index, "refresh_interval", startRefreshInterval);
        }
        return this;
    }

    @Override
    public BulkNodeClient stopBulk(String index) throws IOException {
        if (metric == null) {
            return this;
        }
        if (metric.isBulk(index)) {
            ClientHelper.updateIndexSetting(client, index, "refresh_interval", metric.getStopBulkRefreshIntervals().get(index));
            metric.removeBulk(index);
        }
        return this;
    }

    @Override
    public BulkNodeClient flushIndex(String index) {
        ClientHelper.flushIndex(client, index);
        return this;
    }

    @Override
    public BulkNodeClient refreshIndex(String index) {
        ClientHelper.refreshIndex(client, index);
        return this;
    }

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        return ClientHelper.updateReplicaLevel(client, index, level);
    }


    @Override
    public BulkNodeClient waitForCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        ClientHelper.waitForCluster(client, status, timeout);
        return this;
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        return ClientHelper.waitForRecovery(client, index);
    }

    @Override
    public synchronized void shutdown() {
        try {
            if (bulkProcessor != null) {
                logger.debug("closing bulk processor...");
                bulkProcessor.close();
            }
            if (metric != null && metric.indices() != null && !metric.indices().isEmpty()) {
                logger.debug("stopping bulk mode for indices {}...", metric.indices());
                for (String index : ImmutableSet.copyOf(metric.indices())) {
                    stopBulk(index);
                }
            }
            logger.debug("shutting down...");
            client.close();
            logger.debug("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public BulkNodeClient newIndex(String index) {
        return newIndex(index, (Settings)null, (Map<String,String>)null);
    }

    @Override
    public BulkNodeClient newIndex(String index, String type, InputStream settings,  InputStream mappings) throws IOException {
        configHelper.reset();
        configHelper.setting(settings);
        configHelper.mapping(type, mappings);
        return newIndex(index, configHelper.settings(), configHelper.mappings());
    }

    @Override
    public BulkNodeClient newIndex(String index, Settings settings, Map<String,String> mappings) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client.admin().indices()).setIndex(index);
        if (settings != null) {
            logger.info("settings = {}", settings.getAsStructuredMap());
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mappings != null ) {
            for (String type : mappings.keySet()) {
                logger.info("found mapping for {}", type);
                createIndexRequestBuilder.addMapping(type, mappings.get(type));
            }
        }
        createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created", index);
        return this;
    }

    @Override
    public BulkNodeClient newMapping(String index, String type, Map<String,Object> mapping) {
        PutMappingRequestBuilder putMappingRequestBuilder =
                new PutMappingRequestBuilder(client.admin().indices())
                        .setIndices(index)
                        .setType(type)
                        .setSource(mapping);
        putMappingRequestBuilder.execute().actionGet();
        logger.info("mapping created for index {} and type {}", index, type);
        return this;
    }


    @Override
    public BulkNodeClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                    new DeleteIndexRequestBuilder(client.admin().indices(), index);
        deleteIndexRequestBuilder.execute().actionGet();
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

    public void setSettings(Settings settings) {
        configHelper.settings(settings);
    }

    public Settings getSettings() {
        return configHelper.settings();
    }

    public ImmutableSettings.Builder getSettingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public void setting(InputStream in) throws IOException {
        configHelper.setting(in);
    }

    public void addSetting(String key, String value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Boolean value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Integer value) {
        configHelper.setting(key, value);
    }

    public void mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
    }

    public void mapping(String type, String mapping) throws IOException {
        configHelper.mapping(type, mapping);
    }

    public Map<String, String> getMappings() {
        return configHelper.mappings();
    }

}
