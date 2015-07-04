package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.ConfigHelperTest;
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
import org.xbib.elasticsearch.support.various.NPETest;
import org.xbib.elasticsearch.support.various.SimpleTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        SimpleTest.class,
        ConfigHelperTest.class,
        AliasTest.class,
        NPETest.class,
        BulkNodeClientTest.class,
        BulkNodeDuplicateIDTest.class,
        BulkNodeReplicaTest.class,
        BulkNodeUpdateReplicaLevelTest.class,
        BulkTransportClientTest.class,
        BulkTransportDuplicateIDTest.class,
        BulkTransportReplicaTest.class,
        BulkTransportUpdateReplicaLevelTest.class,
        IngestIndexCreationTest.class,
        IngestClusterBlockTest.class,
        IngestTransportClientTest.class,
        IngestTransportDuplicateIDTest.class,
        IngestTransportReplicaTest.class,
        IngestTransportUpdateReplicaLevelTest.class
})
public class SupportTestSuite {

}
