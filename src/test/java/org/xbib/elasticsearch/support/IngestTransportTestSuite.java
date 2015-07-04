package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        IngestTransportClientTest.class,
})
public class IngestTransportTestSuite {

}
