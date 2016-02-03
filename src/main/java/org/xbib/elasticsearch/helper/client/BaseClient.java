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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class BaseClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseClient.class.getName());

    private Settings.Builder settingsBuilder;

    public abstract ElasticsearchClient client();

    protected abstract void createClient(Settings settings) throws IOException;

    public abstract void shutdown();

    public Settings.Builder getSettingsBuilder() {
        return settingsBuilder();
    }

    public void resetSettings() {
        reset();
    }

    private Settings settings;

    private Map<String, String> mappings = new HashMap<>();

    public void reset() {
        settingsBuilder = Settings.settingsBuilder();
        settings = null;
        mappings = new HashMap<>();
    }

    public void settings(Settings settings) {
        this.settings = settings;
    }

    public void setting(String key, String value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
    }

    public void setting(String key, Boolean value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
    }

    public void setting(String key, Integer value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
    }

    public void setting(InputStream in) throws IOException {
        settingsBuilder = Settings.settingsBuilder().loadFromStream(".json", in);
    }

    public Settings.Builder settingsBuilder() {
        return settingsBuilder != null ? settingsBuilder : Settings.settingsBuilder();
    }

    public Settings settings() {
        if (settings != null) {
            return settings;
        }
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        return settingsBuilder.build();
    }

    public void mapping(String type, String mapping) throws IOException {
        mappings.put(type, mapping);
    }

    public void mapping(String type, InputStream in) throws IOException {
        if (type == null) {
            return;
        }
        StringWriter sw = new StringWriter();
        Streams.copy(new InputStreamReader(in), sw);
        mappings.put(type, sw.toString());
    }

    public Map<String, String> mappings() {
        return mappings.isEmpty() ? null : mappings;
    }


    public void updateIndexSetting(String index, String key, Object value) throws IOException {
        if (client() == null) {
            return;
        }
        if (index == null) {
            throw new IOException("no index name given");
        }
        if (key == null) {
            throw new IOException("no key given");
        }
        if (value == null) {
            throw new IOException("no value given");
        }
        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(settingsBuilder);
        client().execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    public void waitForRecovery() throws IOException {
        if (client() == null) {
            return;
        }
        client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).actionGet();
    }

    public int waitForRecovery(String index) throws IOException {
        if (client() == null) {
            return -1;
        }
        if (index == null) {
            throw new IOException("unable to waitfor recovery, index not set");
        }
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index)).actionGet();
        int shards = response.getTotalShards();
        client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest(index).waitForActiveShards(shards)).actionGet();
        return shards;
    }

    public void waitForCluster(String statusString, TimeValue timeout) throws IOException {
        if (client() == null) {
            return;
        }
        try {
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
            ClusterHealthResponse healthResponse =
                    client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest().waitForStatus(status).timeout(timeout)).actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + " and not " + status.name()
                        + ", from here on, everything will fail!");
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
    }

    public String fetchClusterName() {
        if (client() == null) {
            return null;
        }
        try {
            ClusterStateRequestBuilder clusterStateRequestBuilder =
                    new ClusterStateRequestBuilder(client(), ClusterStateAction.INSTANCE).all();
            ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
            String name = clusterStateResponse.getClusterName().value();
            int nodeCount = clusterStateResponse.getState().getNodes().size();
            return name + " (" + nodeCount + " nodes connected)";
        } catch (ElasticsearchTimeoutException e) {
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            return "DISCONNECTED";
        } catch (Throwable t) {
            return "[" + t.getMessage() + "]";
        }
    }

    public String healthColor() {
        if (client() == null) {
            return null;
        }
        try {
            ClusterHealthResponse healthResponse =
                    client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest().timeout(TimeValue.timeValueSeconds(30))).actionGet();
            ClusterHealthStatus status = healthResponse.getStatus();
            return status.name();
        } catch (ElasticsearchTimeoutException e) {
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            return "DISCONNECTED";
        } catch (Throwable t) {
            return "[" + t.getMessage() + "]";
        }
    }

    public int updateReplicaLevel(String index, int level) throws IOException {
        waitForCluster("YELLOW", TimeValue.timeValueSeconds(30));
        updateIndexSetting(index, "number_of_replicas", level);
        return waitForRecovery(index);
    }

    public void flushIndex(String index) {
        if (client() == null) {
            return;
        }
        if (index != null) {
            client().execute(FlushAction.INSTANCE, new FlushRequest(index)).actionGet();
        }
    }

    public void refreshIndex(String index) {
        if (client() == null) {
            return;
        }
        if (index != null) {
            client().execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
    }

    public void putMapping(String index) {
        if (client() == null) {
            return;
        }
        if (!mappings().isEmpty()) {
            for (Map.Entry<String, String> me : mappings().entrySet()) {
                client().execute(PutMappingAction.INSTANCE,
                        new PutMappingRequest(index).type(me.getKey()).source(me.getValue())).actionGet();
            }
        }
    }

    public String resolveAlias(String alias) {
        if (client() == null) {
            return alias;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }

    public String resolveMostRecentIndex(String alias) {
        if (client() == null) {
            return alias;
        }
        if (alias == null) {
            return null;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> indices = new TreeSet<>(Collections.reverseOrder());
        for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
            Matcher m = pattern.matcher(indexName.value);
            if (m.matches()) {
                if (alias.equals(m.group(1))) {
                    indices.add(indexName.value);
                }
            }
        }
        return indices.isEmpty() ? alias : indices.iterator().next();
    }

    public void switchAliases(String index, String concreteIndex, List<String> extraAliases) {
        switchAliases(index, concreteIndex, extraAliases, null);
    }

    public void switchAliases(String index, String concreteIndex,
                              List<String> extraAliases, IndexAliasAdder adder) {
        if (client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        final List<String> newAliases = new LinkedList<>();
        final List<String> switchedAliases = new LinkedList<>();
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(index).execute().actionGet();
        IndicesAliasesRequestBuilder requestBuilder = new IndicesAliasesRequestBuilder(client(), IndicesAliasesAction.INSTANCE);
        if (getAliasesResponse.getAliases().isEmpty()) {
            //logger.info("adding alias {} to index {}", index, concreteIndex);
            requestBuilder.addAlias(concreteIndex, index);
            newAliases.add(index);
            if (extraAliases != null) {
                for (String extraAlias : extraAliases) {
                    if (adder != null) {
                        adder.addIndexAlias(requestBuilder, concreteIndex, extraAlias);
                    } else {
                        requestBuilder.addAlias(concreteIndex, extraAlias);
                    }
                    newAliases.add(extraAlias);
                }
            }
        } else {
            for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
                if (indexName.value.startsWith(index)) {
                    //logger.info("switching alias {} from index {} to index {}", index, indexName.value, concreteIndex);
                    requestBuilder.removeAlias(indexName.value, index);
                    if (adder != null) {
                        adder.addIndexAlias(requestBuilder, concreteIndex, index);
                    } else {
                        requestBuilder.addAlias(concreteIndex, index);
                    }
                    switchedAliases.add(index);
                    if (extraAliases != null) {
                        for (String extraAlias : extraAliases) {
                            requestBuilder.removeAlias(indexName.value, extraAlias);
                            if (adder != null) {
                                adder.addIndexAlias(requestBuilder, concreteIndex, extraAlias);
                            } else {
                                requestBuilder.addAlias(concreteIndex, extraAlias);
                            }
                            switchedAliases.add(extraAlias);
                        }
                    }
                }
            }
        }
        if (!newAliases.isEmpty() || !switchedAliases.isEmpty()) {
            requestBuilder.execute().actionGet();
            logger.info("new aliases = {}, switched aliases = {}", newAliases, switchedAliases);
        } else {
            logger.info("");
        }
    }

    public void performRetentionPolicy(String index, String concreteIndex, int timestampdiff, int mintokeep) {
        if (client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client(), GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> indices = new TreeSet<>();
        logger.info("{} indices", getIndexResponse.getIndices().length);
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches()) {
                if (index.equals(m.group(1)) && !s.equals(concreteIndex)) {
                    indices.add(s);
                }
            }
        }
        if (indices.isEmpty()) {
            logger.info("no indices found, retention policy skipped");
            return;
        }
        if (mintokeep > 0 && indices.size() < mintokeep) {
            logger.info("{} indices found, not enough for retention policy ({}),  skipped",
                    indices.size(), mintokeep);
            return;
        } else {
            logger.info("candidates for deletion = {}", indices);
        }
        List<String> indicesToDelete = new ArrayList<>();
        // our index
        Matcher m1 = pattern.matcher(concreteIndex);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : indices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = 1 + indices.size() - indicesToDelete.size();
                    if ((timestampdiff == 0 || (timestampdiff > 0 && i1 - i2 > timestampdiff)) && mintokeep <= kept) {
                        indicesToDelete.add(s);
                    }
                }
            }
        }
        logger.info("indices to delete = {}", indicesToDelete);
        if (indicesToDelete.isEmpty()) {
            logger.info("not enough indices found to delete, retention policy complete");
            return;
        }
        String[] s = indicesToDelete.toArray(new String[indicesToDelete.size()]);
        DeleteIndexRequestBuilder requestBuilder = new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, s);
        DeleteIndexResponse response = requestBuilder.execute().actionGet();
        if (!response.isAcknowledged()) {
            logger.warn("retention delete index operation was not acknowledged");
        }
    }

    public Long mostRecentDocument(String index) {
        if (client() == null) {
            return null;
        }
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
        SortBuilder sort = SortBuilders.fieldSort("_timestamp").order(SortOrder.DESC);
        SearchResponse searchResponse = searchRequestBuilder.setIndices(index).addField("_timestamp").setSize(1).addSort(sort).execute().actionGet();
        if (searchResponse.getHits().getHits().length == 1) {
            SearchHit hit = searchResponse.getHits().getHits()[0];
            return hit.getFields().get("_timestamp").getValue();
        }
        return null;
    }

}
