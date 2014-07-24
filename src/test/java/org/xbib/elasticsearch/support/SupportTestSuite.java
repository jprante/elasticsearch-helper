package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.various.AliasTest;
import org.xbib.elasticsearch.support.various.NPETest;
import org.xbib.elasticsearch.support.client.bulk.BulkDuplicateIDTest;
import org.xbib.elasticsearch.support.client.bulk.BulkReplicaTest;
import org.xbib.elasticsearch.support.client.bulk.BulkTransportClientTest;
import org.xbib.elasticsearch.support.client.bulk.BulkUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.ingest.IngestDuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestReplicaTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.IngestUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.node.NodeClientTest;
import org.xbib.elasticsearch.support.client.node.NodeDuplicateIDTest;
import org.xbib.elasticsearch.support.client.node.NodeReplicaTest;
import org.xbib.elasticsearch.support.client.node.NodeUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.ConfigHelperTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ConfigHelperTest.class,
        AliasTest.class,
        NPETest.class,
        NodeClientTest.class,
        NodeDuplicateIDTest.class,
        NodeReplicaTest.class,
        NodeUpdateReplicaLevelTest.class,
        BulkTransportClientTest.class,
        BulkDuplicateIDTest.class,
        BulkReplicaTest.class,
        BulkUpdateReplicaLevelTest.class,
        IngestTransportClientTest.class,
        IngestDuplicateIDTest.class,
        IngestReplicaTest.class,
        IngestUpdateReplicaLevelTest.class
})
public class SupportTestSuite {

}
