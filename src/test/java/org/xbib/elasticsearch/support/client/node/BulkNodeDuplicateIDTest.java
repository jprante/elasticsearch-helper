package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BulkNodeDuplicateIDTest extends AbstractNodeTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkNodeDuplicateIDTest.class.getSimpleName());

    @Test
    public void testDuplicateDocIDs() throws Exception {
        final BulkNodeClient client = new BulkNodeClient()
                .maxActionsPerBulkRequest(1000)
                .newClient(client("1"))
                .newIndex("test");
        try {
            for (int i = 0; i < 12345; i++) {
                client.index("test", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            client.refreshIndex("test");
            long hits = client.client().prepareSearch("test").setTypes("test")
                    .setQuery(matchAllQuery())
                    .execute().actionGet().getHits().getTotalHits();
            logger.info("hits = {}", hits);
            assertTrue(hits < 12345);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
            assertEquals(client.getMetric().getTotalIngest().count(), 13);
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }
}
