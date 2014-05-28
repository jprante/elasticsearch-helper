package org.xbib.elasticsearch.support.helper;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.AliasTest;
import org.xbib.elasticsearch.support.NPETest;
import org.xbib.elasticsearch.support.cron.CronTest;
import org.xbib.elasticsearch.support.client.bulk.BulkTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.DuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.ReplicaLevelTest;
import org.xbib.elasticsearch.support.client.node.NodeClientTest;
import org.xbib.elasticsearch.support.client.ConfigHelperTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ConfigHelperTest.class,
        AliasTest.class,
        NPETest.class,
        CronTest.class,
        ReplicaLevelTest.class,
        DuplicateIDTest.class,
        BulkTransportClientTest.class,
        NodeClientTest.class,
        IngestTransportClientTest.class
})
public class SupportTestSuite {

}
