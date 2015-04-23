package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.ConfigHelperTest;
import org.xbib.elasticsearch.support.client.transport.BulkDuplicateIDTest;
import org.xbib.elasticsearch.support.client.transport.BulkReplicaTest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClientTest;
import org.xbib.elasticsearch.support.client.transport.BulkUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.ingest.IngestClusterBlockTest;
import org.xbib.elasticsearch.support.client.ingest.IngestDuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestIndexCreationTest;
import org.xbib.elasticsearch.support.client.ingest.IngestReplicaTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.IngestUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.node.BulkNodeClientTest;
import org.xbib.elasticsearch.support.client.node.NodeDuplicateIDTest;
import org.xbib.elasticsearch.support.client.node.NodeReplicaTest;
import org.xbib.elasticsearch.support.client.node.NodeUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.various.AliasTest;
import org.xbib.elasticsearch.support.various.NPETest;
import org.xbib.elasticsearch.support.various.SimpleTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        SimpleTest.class,
        ConfigHelperTest.class,
        AliasTest.class,
        NPETest.class,
        BulkNodeClientTest.class,
        NodeDuplicateIDTest.class,
        NodeReplicaTest.class,
        NodeUpdateReplicaLevelTest.class,
        BulkTransportClientTest.class,
        BulkDuplicateIDTest.class,
        BulkReplicaTest.class,
        BulkUpdateReplicaLevelTest.class,
        IngestIndexCreationTest.class,
        IngestClusterBlockTest.class,
        IngestTransportClientTest.class,
        IngestDuplicateIDTest.class,
        IngestReplicaTest.class,
        IngestUpdateReplicaLevelTest.class
})
public class SupportTestSuite {

}
