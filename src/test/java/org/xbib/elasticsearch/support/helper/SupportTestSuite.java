package org.xbib.elasticsearch.support.helper;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.AliasTest;
import org.xbib.elasticsearch.support.NPETest;
import org.xbib.elasticsearch.support.client.bulk.BulkDuplicateIDTest;
import org.xbib.elasticsearch.support.client.bulk.BulkReplicaTest;
import org.xbib.elasticsearch.support.client.bulk.BulkTransportClientTest;
import org.xbib.elasticsearch.support.client.bulk.BulkUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.client.ingest.IngestDuplicateIDTest;
import org.xbib.elasticsearch.support.client.ingest.IngestReplicaTest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClientTest;
import org.xbib.elasticsearch.support.client.ingest.IngestUpdateReplicaLevelTest;
import org.xbib.elasticsearch.support.cron.CronTest;
import org.xbib.elasticsearch.support.client.node.NodeClientTest;
import org.xbib.elasticsearch.support.client.ConfigHelperTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ConfigHelperTest.class,
        AliasTest.class,
        NPETest.class,
        CronTest.class,
        NodeClientTest.class,
        BulkDuplicateIDTest.class,
        BulkReplicaTest.class,
        BulkTransportClientTest.class,
        BulkUpdateReplicaLevelTest.class,
        IngestDuplicateIDTest.class,
        IngestReplicaTest.class,
        IngestTransportClientTest.class,
        IngestUpdateReplicaLevelTest.class
})
public class SupportTestSuite {

}
