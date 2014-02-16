
package org.xbib.elasticsearch.support.client.ingest.index;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.support.AbstractNodeRandomTest;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class IngestIndexClientTests extends AbstractNodeRandomTest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestIndexClientTests.class.getSimpleName());

    @Test
    public void testNewIndexIngest() {
        final IngestIndexClient es = new IngestIndexClient()
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        es.shutdown();
        if (es.hasErrors()) {
            logger.error("error", es.getThrowable());
        }
        assertFalse(es.hasErrors());
    }

    @Test
    public void testDeleteIndexIngestClient() {
        final IngestIndexClient es = new IngestIndexClient()
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            es.deleteIndex()
              .newIndex()
              .deleteIndex();
        } catch (NoNodeAvailableException e) {
            logger.error("no node available");
        } finally {
            es.shutdown();
            if (es.hasErrors()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasErrors());
        }
    }

    @Test
    public void testSingleDocIngestClient() {
        final IngestIndexClient es = new IngestIndexClient()
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            es.deleteIndex();
            es.newIndex();
            es.index("test", "test", "1", new BytesArray("{ \"name\" : \"Jörg Prante\"}").array()); // single doc ingest
            es.flush();
            logger.info("stats={}", es.stats());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            logger.info("total bulk requests = {}", es.getTotalBulkRequests());
            assertEquals(es.getTotalBulkRequests(), 1);
            if (es.hasErrors()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasErrors());
        }
    }

    @Test
    public void testRandomIngestClient() {
        final IngestIndexClient es = new IngestIndexClient()
                .maxActionsPerBulkRequest(1000)
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            for (int i = 0; i < 12345; i++) {
                es.index("test", "test", null, new BytesArray("{ \"name\" : \"" + randomString(32) + "\"}").array());
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            logger.info("total bulk requests = {}", es.getTotalBulkRequests());
            assertEquals(es.getTotalBulkRequests(), 13);
            if (es.hasErrors()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasErrors());
        }
    }

    @Test
    public void testThreadedRandomIngestClient() throws Exception {
        int max = Runtime.getRuntime().availableProcessors();
        final IngestIndexClient es = new IngestIndexClient()
                .maxActionsPerBulkRequest(10000)
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex()
                .startBulk();
        try {
            ThreadPoolExecutor pool =
                    EsExecutors.newScalingExecutorService(max, 30, 1L, TimeUnit.HOURS,
                    EsExecutors.daemonThreadFactory("ingest-test"));
            final CountDownLatch latch = new CountDownLatch(max);
            for (int i = 0; i < max; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < 12345; i++) {
                            es.index("test", "test", null, new BytesArray("{ \"name\" : \"" + randomString(32) + "\"}").array());
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 30 seconds...");
            latch.await(30, TimeUnit.SECONDS);
            pool.shutdown();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("stats={}", es.stats());
            es.stopBulk().shutdown();
            logger.info("total bulk requests = {}", es.getTotalBulkRequests());
            int target = max * 12345 / 10000 + 1;
            assertEquals(es.getTotalBulkRequests(), target);
            if (es.hasErrors()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasErrors());
        }
    }

}
