package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.helper.client.node.BulkNodeClientTest;
import org.xbib.elasticsearch.helper.client.node.BulkNodeDuplicateIDTest;
import org.xbib.elasticsearch.helper.client.node.BulkNodeReplicaTest;
import org.xbib.elasticsearch.helper.client.node.BulkNodeUpdateReplicaLevelTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        BulkNodeClientTest.class,
        BulkNodeDuplicateIDTest.class,
        BulkNodeReplicaTest.class,
        BulkNodeUpdateReplicaLevelTest.class
})
public class BulkNodeTestSuite {

}
