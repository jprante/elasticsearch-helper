package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportClientTest;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportDuplicateIDTest;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportReplicaTest;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportUpdateReplicaLevelTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        BulkTransportClientTest.class,
        BulkTransportDuplicateIDTest.class,
        BulkTransportReplicaTest.class,
        BulkTransportUpdateReplicaLevelTest.class
})
public class BulkTransportTestSuite {

}
