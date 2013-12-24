
package org.xbib.elasticsearch.support;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.support.client.IngestClient;

import java.io.IOException;

public class ReplicaLevelTests extends AbstractNodeRandomTest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(ReplicaLevelTests.class.getSimpleName());

    @Test
    public void testReplicaLevel() throws IOException {

        // we need 3 nodes for replica level 3
        startNode("2");
        startNode("3");

        int numberOfShards = 2;
        int replicaLevel = 3;
        int shardsAfterReplica;

        final IngestClient es = new IngestClient()
                .newClient(getAddress())
                .setIndex("replicatest")
                .setType("replicatest")
                .numberOfShards(numberOfShards)
                .numberOfReplicas(0)
                .newIndex();

        try {
            for (int i = 0; i < 12345; i++) {
                es.indexDocument("replicatest", "replicatest", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.flush();
            shardsAfterReplica = es.updateReplicaLevel(replicaLevel);
            assertEquals(shardsAfterReplica, numberOfShards * (replicaLevel + 1));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            if (es.hasErrors()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasErrors());
        }

        stopNode("3");
        stopNode("2");
    }

}
