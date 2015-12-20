package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xbib.elasticsearch.common.GcMonitor;
import org.xbib.elasticsearch.plugin.helper.HelperPlugin;

import java.util.Collection;
import java.util.List;

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
}
