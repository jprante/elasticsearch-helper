
package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import java.io.IOException;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ReplicaLevelTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(ReplicaLevelTest.class.getSimpleName());

    @Test
    public void testReplicaLevel() throws IOException {

        // we need 3 nodes for replica level 3
        startNode("2");
        startNode("3");

        int numberOfShards = 2;
        int replicaLevel = 3;
        int shardsAfterReplica;

        final IngestTransportClient es = new IngestTransportClient()
                .newClient(getAddress())
                .setIndex("replicatest")
                .setType("replicatest")
                .numberOfShards(numberOfShards)
                .numberOfReplicas(0)
                .newIndex();

        try {
            for (int i = 0; i < 12345; i++) {
                es.index("replicatest", "replicatest", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.flush();
            shardsAfterReplica = es.updateReplicaLevel(replicaLevel);
            assertEquals(shardsAfterReplica, numberOfShards * (replicaLevel + 1));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }

        stopNode("3");
        stopNode("2");
    }

}
