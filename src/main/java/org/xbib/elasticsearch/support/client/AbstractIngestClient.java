
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.config.ConfigHelper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public abstract class AbstractIngestClient extends AbstractTransportClient
        implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(AbstractIngestClient.class.getSimpleName());

    /**
     * The default index
     */
    private String index;
    /**
     * The default type
     */
    private String type;

    private ConfigHelper configHelper = new ConfigHelper();

    @Override
    public AbstractIngestClient setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getIndex() {
        return index;
    }

    @Override
    public AbstractIngestClient setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    @Override
    public AbstractIngestClient waitForCluster() throws IOException {
        return waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
    }

    @Override
    public AbstractIngestClient waitForCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        try {
            logger.info("waiting for cluster state {}", status.name());
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout).execute().actionGet();
            if (healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + " and not " + status.name()
                        + ", cowardly refusing to continue with operations");
            } else {
                logger.info("... cluster state ok");
            }
        } catch (ElasticSearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
        return this;
    }

    @Override
    public int waitForRecovery() {
        if (getIndex() == null) {
            logger.warn("not waiting for recovery, index not set");
            return -1;
        }
        logger.info("waiting for recovery");
        IndicesStatusResponse response = client.admin().indices()
                .status(new IndicesStatusRequest(getIndex()).recovery(true)).actionGet();
        logger.info("waiting for recovery: indices status response = {}, failed = {}", response.getTotalShards(), response.getFailedShards());
        // not all shards are in getShards()
        for (ShardStatus status : response.getShards()) {
            logger.info("recovery: shard {} status {}", status.getShardId(), status.getState().name());
        }
        return response.getTotalShards();
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        if (getIndex() == null) {
            logger.warn("no index name given");
            return -1;
        }
        waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
        update("number_of_replicas", level);
        return waitForRecovery();
    }

    @Override
    public AbstractIngestClient shards(int shards) {
        return setting("index.number_of_shards", shards);
    }

    @Override
    public AbstractIngestClient replica(int replica) {
        return setting("index.number_of_replicas", replica);
    }

    public AbstractIngestClient resetSettings() {
        configHelper.reset();
        return this;
    }

    public AbstractIngestClient setting(String key, String value) {
        configHelper.setting(key, value);
        return this;
    }

    public AbstractIngestClient setting(String key, Boolean value) {
        configHelper.setting(key, value);
        return this;
    }

    public AbstractIngestClient setting(String key, Integer value) {
        configHelper.setting(key, value);
        return this;
    }

    public AbstractIngestClient setting(InputStream in) throws IOException {
        configHelper.setting(in);
        return this;
    }

    public ImmutableSettings.Builder settingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public Settings settings() {
        return configHelper.settings();
    }

    public AbstractIngestClient mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
        return this;
    }

    public AbstractIngestClient mapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
        return this;
    }

    public AbstractIngestClient dateDetection(boolean dateDetection) {
        configHelper.dateDetection(dateDetection);
        return this;
    }

    public boolean dateDetection() {
        return configHelper.dateDetection();
    }

    public AbstractIngestClient timeStampField(String timeStampField) {
        configHelper.timeStampField(timeStampField);
        return this;
    }

    public String timeStampField() {
        return configHelper.timeStampField();
    }

    public AbstractIngestClient timeStampFieldEnabled(boolean enable) {
        configHelper.timeStampFieldEnabled(enable);
        return this;
    }

    public AbstractIngestClient kibanaEnabled(boolean enable) {
        configHelper.kibanaEnabled(enable);
        return this;
    }

    public String defaultMapping() throws IOException {
        return configHelper.defaultMapping();
    }

    public Map<String,String> mappings() {
        return configHelper.mappings();
    }

    protected AbstractIngestClient enableRefreshInterval() {
        update("refresh_interval", 1000);
        return this;
    }

    protected AbstractIngestClient disableRefreshInterval() {
        update("refresh_interval", -1);
        return this;
    }

    @Override
    public AbstractIngestClient newIndex() {
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
                getIndex(), settings() != null ? settings().getAsMap() : null, mappings());
        try {
            client.admin().indices().create(request).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public synchronized AbstractIngestClient deleteIndex() {
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

    public AbstractIngestClient putMapping(String index) {
        if (client == null) {
            logger.warn("no client for put mapping");
            return this;
        }
        configHelper.putMapping(client, index);
        return this;
    }

    public AbstractIngestClient deleteMapping(String index, String type) {
        if (client == null) {
            logger.warn("no client for delete mapping");
            return this;
        }
        configHelper.deleteMapping(client, index, type);
        return this;
    }


    protected void update(String key, Object value) {
        if (client == null) {
            return;
        }
        if (value == null) {
            return;
        }
        if (getIndex() == null) {
            return;
        }
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(getIndex())
                .settings(settingsBuilder);
        client.admin().indices()
                .updateSettings(updateSettingsRequest)
                .actionGet();
    }

}