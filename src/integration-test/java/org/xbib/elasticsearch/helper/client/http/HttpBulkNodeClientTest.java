package org.xbib.elasticsearch.helper.client.http;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.xbib.elasticsearch.helper.client.ClientBuilder;
import org.xbib.elasticsearch.helper.client.HttpBulkNodeClient;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.xbib.elasticsearch.NodeTestUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HttpBulkNodeClientTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(HttpBulkNodeClientTest.class.getSimpleName());

    private final static Integer MAX_ACTIONS = 1000;

    private final static Integer NUM_ACTIONS = 1234;

    @Before
    public void startNodes() {
        try {
            super.startNodes();
            //startNode("2");
            //startNode("3");
            //logger.info("started 3 nodes");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    @Test
    public void testNewIndexNodeClient() throws Exception {
        final HttpBulkNodeClient client = ClientBuilder.builder()
                .put("host", "127.0.0.1")
                .put("port", 9200)
                .setMetric(new LongAdderIngestMetric())
                .toHttpBulkNodeClient();
        client.newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testSingleDocNodeClient() {
        final HttpBulkNodeClient client = ClientBuilder.builder()
                .put("host", "127.0.0.1")
                .put("port", 9200)
                .setMetric(new LongAdderIngestMetric())
                .toHttpBulkNodeClient();
        try {
            client.newIndex("test");
            client.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (InterruptedException e) {
            // ignore
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
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
    public void testRandomDocs() throws Exception {
        final HttpBulkNodeClient client = ClientBuilder.builder()
                .setMetric(new LongAdderIngestMetric())
                .put("host", "127.0.0.1")
                .put("port", 9200)
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .toHttpBulkNodeClient();
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(NUM_ACTIONS / MAX_ACTIONS + 1, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        int maxactions = MAX_ACTIONS;
        final int maxloop = NUM_ACTIONS;
        logger.info("HttpBulkNodeClient max={} maxactions={} maxloop={}", maxthreads, maxactions, maxloop);
        final HttpBulkNodeClient client = ClientBuilder.builder()
                .put("host", "127.0.0.1")
                .put("port", 9200)
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, maxactions)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new LongAdderIngestMetric())
                .toHttpBulkNodeClient();
        try {
            client.newIndex("test")
                    .startBulk("test", -1, 1000);
            ThreadPoolExecutor pool = EsExecutors.newFixed("http-bulk-nodeclient-test", maxthreads, 30,
                    EsExecutors.daemonThreadFactory("http-bulk-nodeclient-test"));
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
            logger.info("flush...");
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            logger.info("got all responses, thread pool shutdown...");
            pool.shutdown();
            logger.info("pool is shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            logger.info("total bulk requests = {}", client.getMetric().getTotalIngest().count());
            assertEquals(maxthreads * maxloop / maxactions + 1, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(maxthreads * maxloop,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.shutdown();
        }
    }

}
