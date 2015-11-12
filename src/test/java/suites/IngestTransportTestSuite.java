package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportDuplicateIDTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportReplicaTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportUpdateReplicaLevelTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        IngestTransportClientTest.class,
        IngestTransportDuplicateIDTest.class,
        IngestTransportReplicaTest.class,
        IngestTransportUpdateReplicaLevelTest.class
})
public class IngestTransportTestSuite {
}
