
package org.xbib.elasticsearch.support;

import org.xbib.elasticsearch.support.client.NodeClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class NodeClientTests extends AbstractNodeTest {

    private final static ESLogger logger = Loggers.getLogger(NodeClientTests.class);

    public void testNodeClient() throws Exception {
        NodeClient es = new NodeClient(client("1"),  INDEX, "test", 10, 10);
        try {
            for (int i = 0; i < 12345; i++) {
                es.indexDocument(null, null, null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        }
    }

}
