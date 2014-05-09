
package org.xbib.elasticsearch.support.client.ingest.index;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IngestIndexClientTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestIndexClientTest.class.getSimpleName());

    @Test
    public void testNewIndexIngest() {
        final IngestIndexTransportClient es = new IngestIndexTransportClient()
                .newClient(getAddress())
                .newIndex("test");
        es.shutdown();
        if (es.hasThrowable()) {
            logger.error("error", es.getThrowable());
        }
        assertFalse(es.hasThrowable());
    }

    @Test
    public void testDeleteIndexIngestClient() {
        final IngestIndexTransportClient es = new IngestIndexTransportClient()
                .newClient(getAddress())
                .newIndex("test");
        try {
            es.deleteIndex("test")
              .newIndex("test")
              .deleteIndex("test");
        } catch (NoNodeAvailableException e) {
            logger.error("no node available");
        } finally {
            es.shutdown();
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

    @Test
    public void testSingleDocIngestClient() {
        final IngestIndexTransportClient es = new IngestIndexTransportClient()
                .newClient(getAddress())
                .newIndex("test");
        try {
            es.deleteIndex("test");
            es.newIndex("test");
            es.index("test", "test", "1", "{ \"name\" : \"Jörg Prante\"}"); // single doc ingest
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            es.shutdown();
            logger.info("total bulk requests = {}", es.getState().getTotalIngest().count());
            assertEquals(1, es.getState().getTotalIngest().count());
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

    @Test
    public void testRandomIngestClient() {
        final IngestIndexTransportClient es = new IngestIndexTransportClient()
                .maxActionsPerBulkRequest(1000)
                .newClient(getAddress())
                .newIndex("test");
        try {
            for (int i = 0; i < 12345; i++) {
                es.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            logger.info("total bulk requests = {}", es.getState().getTotalIngest().count());
            assertEquals(13, es.getState().getTotalIngest().count());
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

    @Test
    public void testThreadedRandomIngestClient() throws Exception {
        int max = Runtime.getRuntime().availableProcessors();
        int maxactions = 1000;
        final int maxloop = 12345;
        final IngestIndexTransportClient es = new IngestIndexTransportClient()
                .maxActionsPerBulkRequest(maxactions)
                .newClient(getAddress())
                .newIndex("test")
                .startBulk("test");
        try {
            ThreadPoolExecutor pool = EsExecutors.newFixed(max, 30,
                    EsExecutors.daemonThreadFactory("ingest-test"));
            final CountDownLatch latch = new CountDownLatch(max);
            for (int i = 0; i < max; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < maxloop; i++) {
                            es.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 60 seconds...");
            latch.await(60, TimeUnit.SECONDS);
            logger.info("shutting down pool...");
            pool.shutdown();
            logger.info("pool shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.stopBulk("test").shutdown();
            logger.info("expected {}, total bulk requests = {}",
                    max * maxloop / maxactions + 1, es.getState().getTotalIngest().count());
            assertEquals( max * maxloop / maxactions + 1, es.getState().getTotalIngest().count());
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

}
