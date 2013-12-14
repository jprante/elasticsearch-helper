
package org.xbib.elasticsearch.support;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.testng.annotations.Test;
import org.elasticsearch.client.transport.NoNodeAvailableException;

import org.xbib.elasticsearch.support.client.NodeClient;

public class NodeClientTests extends AbstractNodeRandomTest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(NodeClientTests.class.getSimpleName());

    @Test
    public void testNewIndexBulkClient() throws Exception {
        final NodeClient es = new NodeClient()
                .flushInterval(TimeValue.timeValueSeconds(5))
                .newClient(client("1"))
                .setIndex(INDEX)
                .setType("test")
                .newIndex();
        es.shutdown();
        if (es.hasErrors()) {
            logger.error("error", es.getThrowable());
        }
        assertFalse(es.hasErrors());
    }

    @Test
    public void testRandomDocsNodeClient() throws Exception {
        final NodeClient es = new NodeClient()
                .maxActionsPerBulkRequest(1000)
                .maxConcurrentBulkRequests(10)
                .flushInterval(TimeValue.timeValueSeconds(5))
                .newClient(client("1"))
                .setIndex(INDEX)
                .setType("test");

        try {
            for (int i = 0; i < 12345; i++) {
                es.indexDocument(null, null, null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            assertEquals(es.getTotalBulkRequests(), 13);
            if (es.hasErrors()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasErrors());
        }
    }

}
