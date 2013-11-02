
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URI;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractIngestClient extends AbstractClient
        implements Ingest, ClientFactory {

    private final static ESLogger logger = Loggers.getLogger(AbstractIngestClient.class);

    private boolean dateDetection = false;

    private boolean timeStampFieldEnabled = false;

    private boolean kibanaEnabled = false;

    private String timeStampField = "@timestamp";

    /**
     * The default index
     */
    private String index;
    /**
     * The default type
     */
    private String type;

    /**
     * A builder for the settings
     */
    private ImmutableSettings.Builder settingsBuilder;

    /**
     * The mappings for the type
     */
    private String mapping;

    public AbstractIngestClient newClient() {
        super.newClient();
        return this;
    }

    public AbstractIngestClient newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    @Override
    public AbstractIngestClient setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public AbstractIngestClient setType(String type) {
        this.type = type;
        return this;
    }

    @Override
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
        IndicesStatusResponse response = client.admin().indices()
                .status(new IndicesStatusRequest(getIndex()).recovery(true)).actionGet();
        logger.info("indices status response = {}, failed = {}", response.getTotalShards(), response.getFailedShards());
        for (ShardStatus status : response.getShards()) {
            logger.info("shard {} status {}", status.getShardId(), status.getState().name());
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

    @Override
    public AbstractIngestClient setting(String key, String value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    @Override
    public AbstractIngestClient setting(String key, Boolean value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    @Override
    public AbstractIngestClient setting(String key, Integer value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    public ImmutableSettings.Builder settings() {
        return settingsBuilder != null ? settingsBuilder : null;
    }

    @Override
    public AbstractIngestClient setting(InputStream in) throws IOException {
        this.settingsBuilder = ImmutableSettings.settingsBuilder().loadFromStream(".json", in);
        return this;
    }

    @Override
    public AbstractIngestClient mapping(InputStream in) throws IOException {
        StringWriter sw = new StringWriter();
        Streams.copy(new InputStreamReader(in), sw);
        this.mapping = sw.toString();
        return this;
    }

    public AbstractIngestClient mapping(String mapping) {
        this.mapping = mapping;
        return this;
    }

    public String mapping() {
        return mapping;
    }

    public AbstractIngestClient dateDetection(boolean dateDetection) {
        this.dateDetection = dateDetection;
        return this;
    }

    public boolean dateDetection() {
        return dateDetection;
    }

    public AbstractIngestClient timeStampField(String timeStampField) {
        this.timeStampField = timeStampField;
        return this;
    }

    public String timeStampField() {
        return timeStampField;
    }

    public AbstractIngestClient timeStampFieldEnabled(boolean enable) {
        this.timeStampFieldEnabled = enable;
        return this;
    }

    public AbstractIngestClient kibanaEnabled(boolean enable) {
        this.kibanaEnabled = enable;
        return this;
    }

    public String defaultMapping() {
        try {
            XContentBuilder b = jsonBuilder()
                            .startObject()
                            .startObject("_default_")
                            .field("date_detection", dateDetection);
            if (timeStampFieldEnabled) {
                            b.startObject("_timestamp")
                            .field("enabled", timeStampFieldEnabled)
                            .field("path", timeStampField)
                            .endObject();
            }
            if (kibanaEnabled) {
                            b.startObject("properties")
                            .startObject("@fields")
                            .field("type", "object")
                            .field("dynamic", true)
                            .field("path", "full")
                            .endObject()
                            .startObject("@message")
                            .field("type", "string")
                            .field("index", "analyzed")
                            .endObject()
                            .startObject("@source")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                            .endObject()
                            .startObject("@source_host")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                            .endObject()
                            .startObject("@source_path")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                            .endObject()
                            .startObject("@tags")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                            .endObject()
                            .startObject("@timestamp")
                            .field("type", "date")
                            .endObject()
                            .startObject("@type")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                            .endObject()
                            .endObject();
            }
            b.endObject()
                    .endObject();
            return b.string();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
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
    public synchronized AbstractIngestClient newIndex() {
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequest request = new CreateIndexRequest(getIndex());
        if (settings() != null && !settings().internalMap().isEmpty()) {
            request.settings(settings());
        }
        if (getType() != null && mapping() != null) {
            request.mapping(getType(), mapping());
        }
        logger.info("creating index = {} type = {} settings = {} mapping = {}",
                getIndex(),
                getType(),
                settings() != null ? settings().build().getAsMap() : null,
                mapping());
        try {
            client.admin().indices()
                    .create(request)
                    .actionGet();
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
            client.admin().indices()
                    .delete(new DeleteIndexRequest(getIndex()))
                    .actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public synchronized AbstractIngestClient newMapping(String type) {
        if (client == null) {
            logger.warn("no client for new mapping");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name for new mapping");
            return this;
        }
        if (mapping() == null) {
            mapping(defaultMapping());
        }
        try {
            client.admin().indices()
                    .putMapping(new PutMappingRequest()
                            .indices(new String[]{getIndex()})
                            .type(getType())
                            .source(mapping()))
                    .actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public synchronized AbstractIngestClient deleteMapping(String type) {
        if (client == null) {
            logger.warn("no client for delete mapping");
            return this;
        }
        if (getIndex() == null) {
            logger.warn("no index name given for delete mapping");
            return this;
        }
        try {
            client.admin().indices()
                    .deleteMapping(new DeleteMappingRequest()
                            .indices(new String[]{getIndex()})
                            .type(type))
                    .actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
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
