package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportDuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportReplicaTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportUpdateReplicaLevelTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        IngestTransportClientTest.class,
        IngestTransportDuplicateIDTest.class,
        IngestTransportReplicaTest.class,
        IngestTransportUpdateReplicaLevelTest.class
})
public class IngestTransportTestSuite {
}
