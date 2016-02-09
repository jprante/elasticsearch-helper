package org.xbib.elasticsearch.helper.client.node;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.xbib.elasticsearch.helper.client.BulkNodeClient;
import org.xbib.elasticsearch.helper.client.ClientBuilder;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.xbib.elasticsearch.NodeTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkNodeUpdateReplicaLevelTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkNodeUpdateReplicaLevelTest.class.getSimpleName());

    @Test
    public void testUpdateReplicaLevel() throws Exception {

        int numberOfShards = 2;
        int replicaLevel = 3;

        // we need 3 nodes for replica level 3
        startNode("2");
        startNode("3");

        int shardsAfterReplica;

        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .build();

        final BulkNodeClient client = ClientBuilder.builder()
                .setMetric(new LongAdderIngestMetric())
                .toBulkNodeClient(client("1"));

        try {
            client.newIndex("replicatest", settings, null);
            for (int i = 0; i < 12345; i++) {
                client.index("replicatest", "replicatest", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            shardsAfterReplica = client.updateReplicaLevel("replicatest", replicaLevel);
            assertEquals(shardsAfterReplica, numberOfShards * (replicaLevel + 1));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }

}
