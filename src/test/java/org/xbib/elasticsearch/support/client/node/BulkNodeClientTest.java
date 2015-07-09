package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkNodeClientTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkNodeClientTest.class.getSimpleName());

    @Test
    public void testNewIndexNodeClient() throws Exception {
        final BulkNodeClient client = new BulkNodeClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(5))
                .init(client("1"))
                .newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testMappingNodeClient() throws Exception {
        final BulkNodeClient client = new BulkNodeClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(5))
                .init(client("1"));
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject("test")
                .startObject("properties")
                .startObject("location")
                .field("type", "geo_point")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client.mapping("test", builder.string());
        client.newIndex("test");
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices("test");
        GetMappingsResponse getMappingsResponse = client.client().admin().indices().getMappings(getMappingsRequest).actionGet();
        logger.info("mappings={}", getMappingsResponse.getMappings());
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testSingleDocNodeClient() {
        final BulkNodeClient client = new BulkNodeClient()
                .maxActionsPerRequest(1000)
                .flushIngestInterval(TimeValue.timeValueSeconds(30))
                .init(client("1"))
                .newIndex("test");
        try {
            client.deleteIndex("test");
            client.newIndex("test");
            client.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (InterruptedException e) {
            // ignore
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
    public void testRandomDocsNodeClient() throws Exception {
        final BulkNodeClient client = new BulkNodeClient()
                .maxActionsPerRequest(1000)
                .flushIngestInterval(TimeValue.timeValueSeconds(10))
                .init(client("1"))
                .newIndex("test");

        try {
            for (int i = 0; i < 12345; i++) {
                client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(13, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocsNodeClient() throws Exception {
        int max = Runtime.getRuntime().availableProcessors();
        int maxactions = 1000;
        final int maxloop = 12345;
        logger.info("NodeClient max={} maxactions={} maxloop={}", max, maxactions, maxloop);
        final BulkNodeClient client = new BulkNodeClient()
                .maxActionsPerRequest(maxactions)
                .flushIngestInterval(TimeValue.timeValueSeconds(600)) // disable auto flush for this test
                .init(client("1"))
                .newIndex("test")
                .startBulk("test");
        try {
            ThreadPoolExecutor pool = EsExecutors.newFixed(max, 30,
                    EsExecutors.daemonThreadFactory("nodeclient-test"));
            final CountDownLatch latch = new CountDownLatch(max);
            for (int i = 0; i < max; i++) {
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
            logger.info("flush...");
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(60));
            logger.info("thread pool shutdown...");
            pool.shutdown();
            logger.info("pool is shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            logger.info("total bulk requests = {}", client.getMetric().getTotalIngest().count());
            assertEquals(max * maxloop / maxactions + 1, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            assertEquals(max * maxloop,
                    client.client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount()
            );
            client.shutdown();
        }
    }

}
