
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public abstract class AbstractIngestTransportClient extends AbstractTransportClient
        implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(AbstractIngestTransportClient.class.getSimpleName());

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
    public AbstractIngestTransportClient setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getIndex() {
        return index;
    }

    @Override
    public AbstractIngestTransportClient setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    @Override
    public AbstractIngestTransportClient shards(int shards) {
        return setting("index.number_of_shards", shards);
    }

    @Override
    public AbstractIngestTransportClient replica(int replica) {
        return setting("index.number_of_replicas", replica);
    }


    public ImmutableSettings.Builder settingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public AbstractIngestTransportClient resetSettings() {
        configHelper.reset();
        return this;
    }

    public AbstractIngestTransportClient setting(String key, String value) {
        configHelper.setting(key, value);
        return this;
    }

    public AbstractIngestTransportClient setting(String key, Boolean value) {
        configHelper.setting(key, value);
        return this;
    }

    public AbstractIngestTransportClient setting(String key, Integer value) {
        configHelper.setting(key, value);
        return this;
    }

    public AbstractIngestTransportClient setting(InputStream in) throws IOException {
        configHelper.setting(in);
        return this;
    }

    public Settings settings() {
        return configHelper.settings();
    }

    public AbstractIngestTransportClient mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
        return this;
    }

    public AbstractIngestTransportClient mapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
        return this;
    }

    public AbstractIngestTransportClient timeStampField(String timeStampField) {
        configHelper.timeStampField(timeStampField);
        return this;
    }

    public String timeStampField() {
        return configHelper.timeStampField();
    }

    public AbstractIngestTransportClient timeStampFieldEnabled(boolean enable) {
        configHelper.timeStampFieldEnabled(enable);
        return this;
    }

    public AbstractIngestTransportClient kibanaEnabled(boolean enable) {
        configHelper.kibanaEnabled(enable);
        return this;
    }

    public String defaultMapping() throws IOException {
        return configHelper.defaultMapping();
    }

    public Map<String,String> mappings() {
        return configHelper.mappings();
    }

    @Override
    public AbstractIngestTransportClient newIndex() {
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
    public synchronized AbstractIngestTransportClient deleteIndex() {
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

    public AbstractIngestTransportClient putMapping(String index) {
        if (client == null) {
            logger.warn("no client for put mapping");
            return this;
        }
        configHelper.putMapping(client, index);
        return this;
    }

    public AbstractIngestTransportClient deleteMapping(String index, String type) {
        if (client == null) {
            logger.warn("no client for delete mapping");
            return this;
        }
        configHelper.deleteMapping(client, index, type);
        return this;
    }

    @Override
    public AbstractIngestTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        ClientHelper.waitForCluster(client, status, timeValue);
        return this;
    }

}
