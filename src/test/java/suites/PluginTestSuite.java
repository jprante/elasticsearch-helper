package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.node.BulkNodeClusterBlockTest;
import org.xbib.elasticsearch.support.various.ConfigHelperTest;
import org.xbib.elasticsearch.support.client.ingest.IngestAutodiscoverTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeClientTest;
import org.xbib.elasticsearch.support.client.ingest.IngestClusterBlockTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportDuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestIndexCreationTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportReplicaTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeDuplicateIDTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeReplicaTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClientTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportDuplicateIDTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportReplicaTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.various.AliasTest;
import org.xbib.elasticsearch.support.various.IngestRequestTest;
import org.xbib.elasticsearch.support.various.SearchTest;
import org.xbib.elasticsearch.support.various.SimpleTest;
import org.xbib.elasticsearch.support.various.WildcardTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        SimpleTest.class,
        ConfigHelperTest.class,
        AliasTest.class,
        IngestRequestTest.class,
        SearchTest.class,
        WildcardTest.class,
        IngestIndexCreationTest.class,
        IngestClusterBlockTest.class,
        IngestAutodiscoverTest.class,
        IngestTransportClientTest.class,
        IngestTransportDuplicateIDTest.class,
        IngestTransportReplicaTest.class,
        IngestTransportUpdateReplicaLevelTest.class,
        BulkNodeClusterBlockTest.class,
        BulkNodeClientTest.class,
        BulkNodeDuplicateIDTest.class,
        BulkNodeReplicaTest.class,
        BulkNodeUpdateReplicaLevelTest.class,
        BulkTransportClientTest.class,
        BulkTransportDuplicateIDTest.class,
        BulkTransportReplicaTest.class,
        BulkTransportUpdateReplicaLevelTest.class
})
public class PluginTestSuite {
}
