/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.elasticsearch.client.support.TransportClientIngestSupport;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.testng.annotations.Test;

import java.io.IOException;

public class ReplicaLevelTests extends AbstractNodeTest {

    private final static ESLogger logger = Loggers.getLogger(ReplicaLevelTests.class);

    @Test
    public void testReplicaLevel() throws IOException {

        int numberOfShards = 1;
        int replicaLevel = 10;

        final TransportClientIngestSupport es = new TransportClientIngestSupport()
                .newClient()
                .index("replicatest")
                .type("replicatest")
                .numberOfShards(numberOfShards)
                .numberOfReplicas(0)
                .newIndex();

        try {
            for (int i = 0; i < 12345; i++) {
                es.index("replicatest", "replicatest", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.flush();
            int shards = es.updateReplicaLevel(replicaLevel);
            assertEquals(shards, numberOfShards * (replicaLevel+1));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }
    }

}
