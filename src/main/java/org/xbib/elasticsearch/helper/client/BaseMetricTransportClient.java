package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

abstract class BaseMetricTransportClient extends BaseTransportClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseMetricTransportClient.class.getName());

    protected IngestMetric metric;

    @Override
    public Ingest init(Settings settings, IngestMetric metric) {
        super.createClient(settings);
        this.metric = metric;
        if (metric != null) {
            metric.start();
        }
        return this;
    }

    @Override
    public BaseMetricTransportClient newIndex(String index) {
        return newIndex(index, null, null);
    }

    @Override
    public BaseMetricTransportClient newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException {
        configHelper.reset();
        configHelper.setting(settings);
        configHelper.mapping(type, mappings);
        return newIndex(index, configHelper.settings(), configHelper.mappings());
    }

    @Override
    public BaseMetricTransportClient newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client(), CreateIndexAction.INSTANCE).setIndex(index);
        if (settings != null) {
            logger.info("settings = {}", settings.getAsStructuredMap());
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mappings != null) {
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
    public BaseMetricTransportClient newMapping(String index, String type, Map<String, Object> mapping) {
        new PutMappingRequestBuilder(client(), PutMappingAction.INSTANCE)
                        .setIndices(index)
                        .setType(type)
                        .setSource(mapping)
                        .execute().actionGet();
        logger.info("mapping created for index {} and type {}", index, type);
        return this;
    }

    @Override
    public synchronized BaseMetricTransportClient deleteIndex(String index) {
        if (client == null) {
            logger.warn("no client for delete index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, index).execute().actionGet();
        return this;
    }

    public BaseMetricTransportClient putMapping(String index) {
        if (client == null) {
            logger.warn("no client for put mapping");
            return this;
        }
        ClientHelper.putMapping(client, configHelper, index);
        return this;
    }

    @Override
    public BaseMetricTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        ClientHelper.waitForCluster(client, status, timeValue);
        return this;
    }

    @Override
    public BaseMetricTransportClient startBulk(String index, long startRefreshIntervalSeconds, long stopRefreshIntervalSeconds) throws IOException {
        if (metric == null) {
            return this;
        }
        if (!metric.isBulk(index)) {
            metric.setupBulk(index, startRefreshIntervalSeconds, stopRefreshIntervalSeconds);
            ClientHelper.updateIndexSetting(client, index, "refresh_interval", startRefreshIntervalSeconds + "s");
        }
        return this;
    }

    @Override
    public BaseMetricTransportClient stopBulk(String index) throws IOException {
        if (metric == null) {
            return this;
        }
        if (metric.isBulk(index)) {
            ClientHelper.updateIndexSetting(client, index, "refresh_interval", metric.getStopBulkRefreshIntervals().get(index) + "s");
            metric.removeBulk(index);
        }
        return this;
    }

}
