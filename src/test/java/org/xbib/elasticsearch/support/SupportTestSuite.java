package org.xbib.elasticsearch.support;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.client.ingest.DuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestClientTest;
import org.xbib.elasticsearch.support.client.ingest.ReplicaLevelTest;
import org.xbib.elasticsearch.support.client.ingest.index.IngestIndexClientTest;
import org.xbib.elasticsearch.support.client.node.NodeClientTest;
import org.xbib.elasticsearch.support.config.ConfigHelperTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ReplicaLevelTest.class,
        ConfigHelperTest.class,
        AliasTest.class,
        NPETest.class,
        IngestClientTest.class,
        IngestIndexClientTest.class,
        DuplicateIDTest.class,
        NodeClientTest.class
})
public class SupportTestSuite {

}
