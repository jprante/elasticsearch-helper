
package org.xbib.elasticsearch.support.client.transport;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkTransportClientTest extends AbstractNodeTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportClientTest.class.getName());

    @Test
    public void testBulkClient() throws IOException {
        final BulkTransportClient client = new BulkTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(30))
                .newClient(getSettings())
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
                .maxActionsPerBulkRequest(1000)
                .flushIngestInterval(TimeValue.timeValueSeconds(30))
                .newClient(getSettings())
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
    public void testRandomDocsBulkClient() throws IOException {
        final BulkTransportClient client = new BulkTransportClient()
                .maxActionsPerBulkRequest(1000)
                .flushIngestInterval(TimeValue.timeValueSeconds(30))
                .newClient(getSettings())
                .newIndex("test");
        try {
            for (int i = 0; i < 12345; i++) {
                client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (InterruptedException e) {
            // ignore
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("bulk requests = {}", client.getMetric().getTotalIngest().count());
            assertEquals(13, client.getMetric().getTotalIngest().count(), 13);
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
        int maxactions = 1000;
        final int maxloop = 12345;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
                .build();

        final BulkTransportClient client = new BulkTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600)) // = disable autoflush for this test
                .maxActionsPerBulkRequest(maxactions)
                .newClient(getSettings())
                .newIndex("test", settings, null)
                .startBulk("test", -1, 1000);
        try {
            ThreadPoolExecutor pool = EsExecutors.newFixed(maxthreads, 30,
                    EsExecutors.daemonThreadFactory("bulkclient-test"));
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
            logger.info("bulk requests = {}", client.getMetric().getTotalIngest().count() );
            assertEquals(maxthreads * maxloop / maxactions + 1, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            assertEquals(maxthreads * maxloop,
                    client.client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount()
            );
            client.shutdown();
        }
    }

}
