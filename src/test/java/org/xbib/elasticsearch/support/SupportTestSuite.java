package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        /*SimpleTest.class,
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
        IngestClusterBlockTest.class,*/
        IngestTransportClientTest.class/*,
        IngestDuplicateIDTest.class,
        IngestReplicaTest.class,
        IngestUpdateReplicaLevelTest.class*/
})
public class SupportTestSuite {

}
