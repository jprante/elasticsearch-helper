package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.node.BulkNodeClientTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        BulkNodeClientTest.class,
})
public class BulkNodeTestSuite {

}
