package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClientTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        BulkTransportClientTest.class,
})
public class SingleTestSuite {

}
