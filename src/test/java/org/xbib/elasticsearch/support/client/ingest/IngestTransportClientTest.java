
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

    @Test
    public void testNewIndexIngest() {
        final IngestTransportClient es = new IngestTransportClient()
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
        final IngestTransportClient es = new IngestTransportClient()
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
        final IngestTransportClient es = new IngestTransportClient()
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
            assertEquals(es.getState().getTotalIngest().count(), 1);
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

    @Test
    public void testRandomDocsIngestClient() {
        final IngestTransportClient es = new IngestTransportClient()
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
            assertEquals(es.getState().getTotalIngest().count(), 13);
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

    @Test
    public void testThreadedRandomDocsIngestClient() throws Exception {
        int max = Runtime.getRuntime().availableProcessors();
        int maxactions = 1000;
        final int maxloop = 12345;
        final IngestTransportClient client = new IngestTransportClient()
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
                            client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 60 seconds...");
            latch.await(60, TimeUnit.SECONDS);
            logger.info("client flush ...");
            client.flush();
            client.waitForResponses(TimeValue.timeValueSeconds(60));
            logger.info("test thread pool to be shut down ...");
            pool.shutdown();
            logger.info("thread poot shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            logger.info("total bulk requests = {}", client.getState().getTotalIngest().count());
            assertEquals(max * maxloop / maxactions + 1, client.getState().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refresh("test");
            assertEquals(max * maxloop,
                    client.client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount()
            );
            client.shutdown();
        }
    }

}
