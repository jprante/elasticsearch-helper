package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.junit.Test;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DuplicateIDTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(DuplicateIDTest.class.getSimpleName());

    @Test
    public void testDuplicateDocIDs() {
        final IngestClient es = new IngestClient()
                .maxActionsPerBulkRequest(1000)
                .newClient(getAddress())
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            for (int i = 0; i < 12345; i++) {
                es.index("test", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            logger.info("submitted={} succedded={}", es.getTotalSubmitted(), es.getTotalSucceeded());
            assertEquals(es.getTotalBulkRequests(), 13);
            if (es.hasErrors()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasErrors());
        }
    }
}
