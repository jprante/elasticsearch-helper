
package org.xbib.elasticsearch.helper.client.transport;

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
import org.xbib.elasticsearch.helper.client.BulkTransportClient;
import org.xbib.elasticsearch.helper.client.ClientBuilder;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkTransportClientTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportClientTest.class.getName());

    private final static Long MAX_ACTIONS = 1000L;

    private final static Long NUM_ACTIONS = 1234L;


    @Before
    public void startNodes() {
        try {
            super.startNodes();
            startNode("2");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    @Test
    public void testBulkClient() throws IOException {
        final BulkTransportClient client = ClientBuilder.builder()
                .put(getSettings())
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new LongAdderIngestMetric())
                .toBulkTransportClient();
        client.newIndex("test");
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
        final BulkTransportClient client = ClientBuilder.builder()
                .put(getSettings())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new LongAdderIngestMetric())
                .toBulkTransportClient();
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
            assertEquals(1, client.getMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testRandomDocsBulkClient() throws IOException {
        long numactions = NUM_ACTIONS;
        final BulkTransportClient client = ClientBuilder.builder()
                .put(getSettings())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new LongAdderIngestMetric())
                .toBulkTransportClient();
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
            assertEquals(numactions, client.getMetric().getSucceeded().getCount());
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
        long maxactions = MAX_ACTIONS;
        final long maxloop = NUM_ACTIONS;

        Settings settingsForIndex = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();

        final BulkTransportClient client = ClientBuilder.builder()
                .put(getSettings())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, maxactions)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60)) // = disable autoflush for this test
                .setMetric(new LongAdderIngestMetric())
                .toBulkTransportClient();
        try {
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
            logger.info("waiting for max 30 seconds...");
            latch.await(30, TimeUnit.SECONDS);
            logger.info("client flush ...");
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            logger.info("thread pool to be shut down ...");
            pool.shutdown();
            logger.info("poot shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            assertEquals(maxthreads * maxloop, client.getMetric().getSucceeded().getCount());
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
