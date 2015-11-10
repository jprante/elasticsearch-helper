package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.helper.client.ConfigHelperTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestDuplicateIDTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestIndexCreationTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportReplicaTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportUpdateReplicaLevelTest;
import org.xbib.elasticsearch.helper.client.node.BulkNodeClientTest;
import org.xbib.elasticsearch.helper.client.node.BulkNodeDuplicateIDTest;
import org.xbib.elasticsearch.helper.client.node.BulkNodeReplicaTest;
import org.xbib.elasticsearch.helper.client.node.BulkNodeUpdateReplicaLevelTest;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportReplicaTest;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportClientTest;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportDuplicateIDTest;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportUpdateReplicaLevelTest;
import org.xbib.elasticsearch.helper.various.AliasTest;
import org.xbib.elasticsearch.helper.various.NPETest;
import org.xbib.elasticsearch.helper.various.SimpleTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        SimpleTest.class,
        ConfigHelperTest.class,
        AliasTest.class,
        NPETest.class,
        //IngestClusterBlockTest.class,
        BulkNodeClientTest.class,
        BulkNodeDuplicateIDTest.class,
        BulkNodeReplicaTest.class,
        BulkNodeUpdateReplicaLevelTest.class,
        BulkTransportClientTest.class,
        BulkTransportDuplicateIDTest.class,
        BulkTransportReplicaTest.class,
        BulkTransportUpdateReplicaLevelTest.class,
        IngestIndexCreationTest.class,
        IngestTransportClientTest.class,
        IngestDuplicateIDTest.class,
        IngestTransportReplicaTest.class,
        IngestTransportUpdateReplicaLevelTest.class
})
public class HelperTestSuite {

}
