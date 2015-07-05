package org.xbib.elasticsearch.support.client;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public class ClientHelper {

    public static void updateIndexSetting(Client client, String index, String key, Object value) throws IOException {
        if (client == null) {
            throw new IOException("no client");
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
        client.admin().indices().updateSettings(updateSettingsRequest).actionGet();
    }

    public static int waitForRecovery(Client client, String index) throws IOException {
        if (index == null) {
            throw new IOException("unable to waitfor recovery, index not set");
        }
        RecoveryResponse response = client.admin().indices().prepareRecoveries(index).execute().actionGet();
        int shards = response.getTotalShards();
        client.admin().cluster().prepareHealth(index).setWaitForActiveShards(shards).execute().actionGet();
        return shards;
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

    public static String clusterName(ElasticsearchClient client) {
        try {
            ClusterStateRequestBuilder clusterStateRequestBuilder =
                    new ClusterStateRequestBuilder(client, ClusterStateAction.INSTANCE).all();
            ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
            String name = clusterStateResponse.getClusterName().value();
            int nodeCount = clusterStateResponse.getState().getNodes().size();
            return  name + " (" + nodeCount + " nodes connected)";
        } catch (ElasticsearchTimeoutException e) {
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            return "DISCONNECTED";
        } catch (Throwable t) {
            return "[" + t.getMessage() + "]";
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
        updateIndexSetting(client, index, "number_of_replicas", level);
        return waitForRecovery(client, index);
    }

    public static void flushIndex(Client client, String index) {
        if (client != null && index != null) {
            client.admin().indices().prepareFlush().setIndices(index).execute().actionGet();
        }
    }

    public static void refreshIndex(Client client, String index) {
        if (client != null && index != null) {
            client.admin().indices().prepareRefresh().setIndices(index).execute().actionGet();
        }
    }

}
