
package org.xbib.elasticsearch.helper.client.ingest;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;

import org.junit.Test;
import org.xbib.elasticsearch.util.NodeTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IngestTransportUpdateReplicaLevelTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportUpdateReplicaLevelTest.class.getSimpleName());

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

        final IngestTransportClient ingest = new IngestTransportClient()
                .init(getSettings(), new LongAdderIngestMetric())
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
