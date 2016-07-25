/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

abstract class BaseMetricTransportClient extends BaseTransportClient implements ClientAPI {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseMetricTransportClient.class.getName());

    protected IngestMetric metric;

    @Override
    public ClientAPI init(Settings settings, IngestMetric metric) {
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
        resetSettings();
        setting(settings);
        mapping(type, mappings);
        return newIndex(index, settings(), mappings());
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

    @Override
    public BaseMetricTransportClient startBulk(String index, long startRefreshIntervalSeconds, long stopRefreshIntervalSeconds) throws IOException {
        if (metric == null) {
            return this;
        }
        if (!metric.isBulk(index)) {
            metric.setupBulk(index, startRefreshIntervalSeconds, stopRefreshIntervalSeconds);
            updateIndexSetting(index, "refresh_interval", startRefreshIntervalSeconds + "s");
        }
        return this;
    }

    @Override
    public BaseMetricTransportClient stopBulk(String index) throws IOException {
        if (metric == null) {
            return this;
        }
        if (metric.isBulk(index)) {
            updateIndexSetting(index, "refresh_interval", metric.getStopBulkRefreshIntervals().get(index) + "s");
            metric.removeBulk(index);
        }
        return this;
    }

}
