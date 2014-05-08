
package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkTransportClientTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportClientTest.class.getName());

    @Test
    public void testBulkClient() {
        final BulkTransportClient es = new BulkTransportClient()
                .flushInterval(TimeValue.timeValueSeconds(5))
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        es.shutdown();
        if (es.hasThrowable()) {
            logger.error("error", es.getThrowable());
        }
        assertFalse(es.hasThrowable());
        try {
            es.deleteIndex()
              .newIndex()
              .deleteIndex();
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
    public void testSingleDocBulkClient() {
        /**
         * most of the times, BulkProcessor has difficulties with a single document...
         */
        final BulkTransportClient es = new BulkTransportClient()
                .maxActionsPerBulkRequest(1000)
                .flushInterval(TimeValue.timeValueSeconds(5))
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            es.deleteIndex();
            es.newIndex();
            es.index("test", "test", "1", "{ \"name\" : \"Jörg Prante\"}"); // single doc ingest
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            logger.info("bulk requests = {}", es.getState().getTotalIngest().count());
            assertEquals(1, es.getState().getTotalIngest().count());
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

    @Test
    public void testRandomDocsBulkClient() {
        final BulkTransportClient es = new BulkTransportClient()
                .maxActionsPerBulkRequest(1000)
                .flushInterval(TimeValue.timeValueSeconds(10))
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            for (int i = 0; i < 12345; i++) {
                es.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            logger.info("bulk requests = {}", es.getState().getTotalIngest().count());
            assertEquals(13, es.getState().getTotalIngest().count(), 13);
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

    @Test
    public void testThreadedRandomDocsBulkClient() throws Exception {
        final BulkTransportClient client = new BulkTransportClient()
                .flushInterval(TimeValue.timeValueSeconds(5))
                .maxActionsPerBulkRequest(1000)
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex()
                .startBulk();
        int max = Runtime.getRuntime().availableProcessors();
        try {
            ThreadPoolExecutor pool = EsExecutors.newFixed(max, 30, EsExecutors.daemonThreadFactory("bulk-test"));
            final CountDownLatch latch = new CountDownLatch(max);
            for (int i = 0; i < max; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < 12345; i++) {
                            client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for {} seconds...", client.flushInterval().getSeconds());
            latch.await(client.flushInterval().getSeconds(), TimeUnit.SECONDS);
            pool.shutdown();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk().flush().shutdown();
            logger.info("bulk requests = {}", client.getState().getTotalIngest().count() );
            int target = max * 12345 / 1000 + 1;
            assertEquals(target, client.getState().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }

}
