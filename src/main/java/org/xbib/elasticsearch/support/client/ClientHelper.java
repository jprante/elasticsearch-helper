
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ClientHelper {

    public static List<String> getConnectedNodes(TransportClient client) {
        List<String> nodes = newLinkedList();
        if (client.connectedNodes() != null) {
            for (DiscoveryNode discoveryNode : client.connectedNodes()) {
                nodes.add(discoveryNode.toString());
            }
        }
        return nodes;
    }

    public static String threadPoolStats(TransportClient client) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        client.threadPool().stats().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.string();
    }

    public static void updateIndexSetting(Client client, String index, String key, Object value) throws IOException {
        if (client == null) {
            throw new IOException("no client");
        }
        if (index == null) {
            throw new IOException("no index name given");
        }
        if (value == null) {
            throw new IOException("no value given");
        }
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(settingsBuilder);
        client.admin().indices().updateSettings(updateSettingsRequest).actionGet();
    }

    public static int waitForRecovery(Client client, String index) throws IOException {
        if (index == null) {
            throw new IOException("unable to waitfor recovery, index not set");
        }
        IndicesStatusResponse response = client.admin().indices()
                .status(new IndicesStatusRequest(index).recovery(true)).actionGet();
        return response.getTotalShards();
    }

    public static void waitForCluster(Client client, ClusterHealthStatus status, TimeValue timeout) throws IOException {
        try {
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout).execute().actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + " and not " + status.name()
                        + ", cowardly refusing to continue with operations");
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
    }

    public static String healthColor(Client client) {
        try {
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setTimeout(TimeValue.timeValueSeconds(30)).execute().actionGet();
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

    public static int updateReplicaLevel(Client client, String index, int level) throws IOException {
        waitForCluster(client, ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
        waitForRecovery(client, index);
        updateIndexSetting(client, index, "number_of_replicas", level);
        return waitForRecovery(client, index);
    }

    public static void startBulk(Client client, String index) throws IOException {
        updateIndexSetting(client, index, "refresh_interval", -1);
        updateReplicaLevel(client, index, 0);
    }

    public static void stopBulk(Client client, String index) throws IOException {
        updateIndexSetting(client, index, "refresh_interval", 1000);
    }

    public static void refresh(Client client, String index) {
        client.admin().indices().refresh(new RefreshRequest(index));
    }

}
