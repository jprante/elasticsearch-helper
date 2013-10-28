
package org.elasticsearch.test;

import org.xbib.elasticsearch.support.client.AbstractIngestClient;
import org.xbib.elasticsearch.support.client.IngestClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.testng.annotations.Test;

import java.io.IOException;

public class ReplicaLevelTests extends AbstractNodeTest {

    private final static ESLogger logger = Loggers.getLogger(ReplicaLevelTests.class);

    @Test
    public void testReplicaLevel() throws IOException {

        int numberOfShards = 5;
        int replicaLevel = 4;
        int shardsAfterReplica = 0;

        final AbstractIngestClient es = new IngestClient()
                .newClient(ADDRESS)
                .setIndex("replicatest")
                .setType("replicatest")
                .numberOfShards(numberOfShards)
                .numberOfReplicas(0)
                .dateDetection(false)
                .timeStampFieldEnabled(false)
                .newIndex(false);

        try {
            for (int i = 0; i < 12345; i++) {
                es.indexDocument("replicatest", "replicatest", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.flush();
            shardsAfterReplica = es.updateReplicaLevel(replicaLevel);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            //assertEquals(shardsAfterReplica, numberOfShards * (replicaLevel + 1));
            es.shutdown();
        }
    }

}
