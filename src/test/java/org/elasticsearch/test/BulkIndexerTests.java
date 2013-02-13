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

import org.elasticsearch.action.bulk.support.ElasticsearchIndexer;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BulkIndexerTests extends AbstractNodeTest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkIndexerTests.class.getName());

    @Test
    public void testDeleteIndex() {

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(defaultSettings)
                .newClient()
                .index("test")
                .type("test");

        try {
            es.deleteIndex();
            es.index();
            es.deleteIndex();
        } catch (NoNodeAvailableException e) {
            // if no node, just skip
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testSimpleBulk() {

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(defaultSettings)
                .newClient()
                .index("test")
                .type("test");
        try {
            es.deleteIndex();
            es.index(); // create index
            es.index("test", "test", "1", "{ \"name\" : \"Jörg Prante\"}"); // single doc bulk
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testRandomBulk() {

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(defaultSettings)
                .newClient();
        try {
            for (int i = 0; i < 12345; i++) {
                es.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testThreadedBulk() throws Exception {
        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .newClient();
        try {
            int min = 0;
            int max = 4;
            ThreadPoolExecutor pool = EsExecutors.newScalingExecutorService(min, max, 100, TimeUnit.DAYS,
                    EsExecutors.daemonThreadFactory("bulktest"));
            final CountDownLatch latch = new CountDownLatch(max);
            for (int i = 0; i < max; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                            for (int i = 0; i < 12345; i++) {
                                es.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                            }
                            logger.info("done");
                            latch.countDown();
                    }
                });
            }
            logger.info("waiting for 30 seconds...");
            latch.await(30, TimeUnit.SECONDS);
            pool.shutdown();
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }

    }

}
