
package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IngestTransportClientTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportClientTest.class.getSimpleName());


    public void testNewIndexIngest() {
        final IngestTransportClient ingest = new IngestTransportClient()
                .newClient(getAddress())
                .shards(2)
                .replica(0)
                .newIndex("test");
        ingest.shutdown();
        if (ingest.hasThrowable()) {
            logger.error("error", ingest.getThrowable());
        }
        assertFalse(ingest.hasThrowable());
    }


    public void testDeleteIndexIngestClient() {
        final IngestTransportClient ingest = new IngestTransportClient()
                .newClient(getAddress())
                .shards(2)
                .replica(0)
                .newIndex("test");
        try {
            ingest.deleteIndex("test")
              .newIndex("test")
              .deleteIndex("test");
        } catch (NoNodeAvailableException e) {
            logger.error("no node available");
        } finally {
            ingest.shutdown();
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
        }
    }

    @Test
    public void testSingleDocIngestClient() {
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .newClient(getAddress())
                .shards(2)
                .replica(0)
                .newIndex("test");
        try {
            ingest.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (InterruptedException e) {
            // ignore
        } finally {
            logger.info("total bulk requests = {}", ingest.getState().getTotalIngest().count());
            assertEquals(1, ingest.getState().getTotalIngest().count());
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
            ingest.shutdown();
        }
    }


    public void testRandomDocsIngestClient() throws Exception {
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .maxActionsPerBulkRequest(1000)
                .newClient(getAddress())
                .shards(2)
                .replica(0)
                .newIndex("test")
                .startBulk("test");
        try {
            for (int i = 0; i < 12345; i++) {
                ingest.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (InterruptedException e) {
            // ignore
        } finally {
            ingest.stopBulk("test");
            logger.info("total requests = {}", ingest.getState().getTotalIngest().count());
            assertEquals(13, ingest.getState().getTotalIngest().count());
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
            ingest.shutdown();
        }
    }


    public void testThreadedRandomDocsIngestClient() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        int maxactions = 1000;
        final int maxloop = 12345;
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .maxActionsPerBulkRequest(maxactions)
                .newClient(getAddress())
                .shards(2)
                .replica(0)
                .newIndex("test")
                .startBulk("test");
        try {
            ThreadPoolExecutor pool = EsExecutors.newFixed(maxthreads, 30,
                    EsExecutors.daemonThreadFactory("ingest-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < maxloop; i++) {
                            ingest.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 60 seconds...");
            latch.await(60, TimeUnit.SECONDS);
            logger.info("client flush ...");
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(30));
            logger.info("thread pool to be shut down ...");
            pool.shutdown();
            logger.info("thread pool shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            ingest.stopBulk("test");
            logger.info("total requests = {}", ingest.getState().getTotalIngest().count());
            assertEquals(maxthreads * maxloop / maxactions + 1, ingest.getState().getTotalIngest().count());
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
            ingest.refresh("test");
            assertEquals(maxthreads * maxloop,
                    ingest.client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount()
            );
            ingest.shutdown();
        }
    }

}
