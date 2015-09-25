package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkNodeUpdateReplicaLevelTest extends AbstractNodeTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkNodeUpdateReplicaLevelTest.class.getSimpleName());

    @Test
    public void testUpdateReplicaLevel() throws Exception {

        int numberOfShards = 2;
        int replicaLevel = 3;

        // we need 3 nodes for replica level 3
        startNode("2");
        startNode("3");

        int shardsAfterReplica;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .build();

        final BulkNodeClient ingest = new BulkNodeClient()
                .newClient(client("1"))
                .newIndex("replicatest", settings, null);

        ingest.waitForCluster(ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30));

        try {
            for (int i = 0; i < 12345; i++) {
                ingest.index("replicatest", "replicatest", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(30));
            shardsAfterReplica = ingest.updateReplicaLevel("replicatest", replicaLevel);
            assertEquals(shardsAfterReplica, numberOfShards * (replicaLevel + 1));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            ingest.deleteIndex("replicatest");
            ingest.shutdown();
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
        }
    }

}
