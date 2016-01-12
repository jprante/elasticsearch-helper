package org.xbib.elasticsearch.helper.client;

import com.carrotsearch.hppc.cursors.ObjectCursor;
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
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.xbib.elasticsearch.common.GcMonitor;
import org.xbib.elasticsearch.plugin.helper.HelperPlugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class BaseTransportClient extends BaseClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseTransportClient.class.getName());

    protected TransportClient client;

    protected GcMonitor gcmon;

    protected boolean ignoreBulkErrors;

    private boolean isShutdown;

    @Override
    protected void createClient(Settings settings) {
        if (client != null) {
            logger.warn("client is open, closing...");
            client.close();
            client.threadPool().shutdown();
            logger.warn("client is closed");
            client = null;
        }
        if (settings != null) {
            String version = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.runtime.version")
                    + " " + System.getProperty("java.vm.version");
            logger.info("creating transport client on {} with effective settings {}",
                    version, settings.getAsMap());
            this.client = TransportClient.builder()
                    .addPlugin(HelperPlugin.class)
                    .settings(settings)
                    .build();
            this.gcmon = new GcMonitor(settings);
            this.ignoreBulkErrors = settings.getAsBoolean("ignoreBulkErrors", true);
        }
    }

    @Override
    public ElasticsearchClient client() {
        return client;
    }

    public synchronized void shutdown() {
        if (client != null) {
            logger.debug("shutdown started");
            client.close();
            client.threadPool().shutdown();
            client = null;
            logger.debug("shutdown complete");
        }
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    protected boolean connect(Collection<InetSocketTransportAddress> addresses, boolean autodiscover) {
        logger.info("trying to connect to {}", addresses);
        for (InetSocketTransportAddress address : addresses) {
            client.addTransportAddress(address);
        }
        if (client.connectedNodes() != null) {
            List<DiscoveryNode> nodes = client.connectedNodes();
            if (!nodes.isEmpty()) {
                logger.info("connected to {}", nodes);
                if (autodiscover) {
                    logger.info("trying to auto-discover all cluster nodes...");
                    ClusterStateRequestBuilder clusterStateRequestBuilder =
                            new ClusterStateRequestBuilder(client, ClusterStateAction.INSTANCE);
                    ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
                    DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                    for (DiscoveryNode node : discoveryNodes) {
                        logger.info("connecting to auto-discovered node {}", node);
                        try {
                            client.addTransportAddress(node.address());
                        } catch (Exception e) {
                            logger.warn("can't connect to node " + node, e);
                        }
                    }
                    logger.info("after auto-discovery connected to {}", client.connectedNodes());
                }
                return true;
            }
            return false;
        }
        return false;
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

    public void switchAliases(String index, String concreteIndex, List<String> extraAliases) {
        if (client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(index).execute().actionGet();
        IndicesAliasesRequestBuilder requestBuilder = new IndicesAliasesRequestBuilder(client(), IndicesAliasesAction.INSTANCE);
        if (getAliasesResponse.getAliases().isEmpty()) {
            logger.info("adding alias {} to index {}", index, concreteIndex);
            requestBuilder.addAlias(concreteIndex, index);
            if (extraAliases != null) {
                for (String extraAlias : extraAliases) {
                    requestBuilder.addAlias(concreteIndex, extraAlias);
                }
            }
        } else {
            for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
                if (indexName.value.startsWith(index)) {
                    logger.info("switching alias {} from index {} to index {}", index, indexName.value, concreteIndex);
                    requestBuilder.removeAlias(indexName.value, index)
                            .addAlias(concreteIndex, index);
                    if (extraAliases != null) {
                        for (String extraAlias : extraAliases) {
                            requestBuilder.removeAlias(indexName.value, extraAlias)
                                    .addAlias(concreteIndex, extraAlias);
                        }
                    }
                }
            }
        }
        requestBuilder.execute().actionGet();
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
        List<String> indices = new ArrayList<>();
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
        List<String> indicesToDelete = new ArrayList<String>();
        // our index
        Matcher m1 = pattern.matcher(concreteIndex);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : indices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = 1 + indices.size() - indicesToDelete.size();
                    if (timestampdiff > 0 && i1 - i2 > timestampdiff && mintokeep <= kept) {
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

    public void findMostRecentDocument(String index) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
        SortBuilder sort = SortBuilders.fieldSort("_timestamp").order(SortOrder.DESC);
        SearchResponse searchResponse = searchRequestBuilder.setIndices(index).addField("_timestamp").setSize(1).addSort(sort).execute().actionGet();
        if (searchResponse.getHits().getHits().length == 1) {
            SearchHit hit = searchResponse.getHits().getHits()[0];
            Long timestamp = hit.getFields().get("_timestamp").getValue();
            SimpleDateFormat sdf = new SimpleDateFormat();
            logger.info("most recent document timestamp is {}",sdf.format(new Date(timestamp)));
        }
    }
}
