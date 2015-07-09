package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.ConfigHelperTest;
import org.xbib.elasticsearch.support.client.ingest.IngestClusterBlockTest;
import org.xbib.elasticsearch.support.client.ingest.IngestDuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestIndexCreationTest;
import org.xbib.elasticsearch.support.client.ingest.IngestReplicaTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.IngestUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeClientTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeDuplicateIDTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeReplicaTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportReplicaTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClientTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportDuplicateIDTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.various.AliasTest;
import org.xbib.elasticsearch.support.various.NPETest;
import org.xbib.elasticsearch.support.various.SimpleTest;

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
        IngestReplicaTest.class,
        IngestUpdateReplicaLevelTest.class
})
public class SupportTestSuite {

}
