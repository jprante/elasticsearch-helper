
package org.xbib.elasticsearch.helper.client.transport;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.xbib.elasticsearch.helper.client.ClientHelper;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.xbib.elasticsearch.util.NodeTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkTransportClientTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportClientTest.class.getName());

    private final static Integer MAX_ACTIONS = 10000;

    private final static Integer NUM_ACTIONS = 12345;


    @Before
    public void startNodes() {
        try {
            super.startNodes();
            startNode("2");
            startNode("3");
            logger.info("started 3 nodes");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    // disabled because of https://github.com/elastic/elasticsearch/issues/15225
    public void testBulkClient() throws IOException {
        final BulkTransportClient client = new BulkTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(30))
                .init(getSettings(), new LongAdderIngestMetric())
                .newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        try {
            client.deleteIndex("test")
              .newIndex("test")
              .deleteIndex("test");
        } catch (NoNodeAvailableException e) {
            logger.error("no node available");
        } finally {
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testSingleDocBulkClient() throws IOException {
        final BulkTransportClient client = new BulkTransportClient()
                .maxActionsPerRequest(MAX_ACTIONS)
                .flushIngestInterval(TimeValue.timeValueSeconds(30))
                .init(getSettings(), new LongAdderIngestMetric());
        try {
            client.newIndex("test");
            client.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (InterruptedException e) {
            // ignore
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("bulk requests = {}", client.getMetric().getTotalIngest().count());
            assertEquals(1, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testRandomDocsBulkClient() throws IOException {
        final BulkTransportClient client = new BulkTransportClient()
                .maxActionsPerRequest(MAX_ACTIONS)
                .flushIngestInterval(TimeValue.timeValueSeconds(30))
                .init(getSettings(), new LongAdderIngestMetric());
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (InterruptedException e) {
            // ignore
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("bulk requests = {}", client.getMetric().getTotalIngest().count());
            assertEquals(NUM_ACTIONS / MAX_ACTIONS + 1, client.getMetric().getTotalIngest().count(), 13);
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocsBulkClient() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        int maxactions = MAX_ACTIONS;
        final int maxloop = NUM_ACTIONS;

        Settings settingsForIndex = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();

        final BulkTransportClient client = new BulkTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600)) // = disable autoflush for this test
                .maxActionsPerRequest(maxactions)
                .init(getSettings(), new LongAdderIngestMetric());
        try {
            ClientHelper.waitForCluster(client.client(), ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(10));
            client.newIndex("test", settingsForIndex, null)
                    .startBulk("test", -1, 1000);
            ThreadPoolExecutor pool =
                    EsExecutors.newFixed("bulkclient-test", maxthreads, 30, EsExecutors.daemonThreadFactory("bulkclient-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < maxloop; i++) {
                            client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 60 seconds...");
            latch.await(60, TimeUnit.SECONDS);
            logger.info("client flush ...");
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(60));
            logger.info("thread pool to be shut down ...");
            pool.shutdown();
            logger.info("poot shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            logger.info("bulk requests = {}", client.getMetric().getTotalIngest().count());
            assertEquals(maxthreads * maxloop / maxactions + 1, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("_all") // to avoid NPE at org.elasticsearch.action.search.SearchRequest.writeTo(SearchRequest.java:580)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0);
            assertEquals(maxthreads * maxloop,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.shutdown();
        }
    }

}
