package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.support.various.ConfigHelperTest;
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
        BulkNodeTestSuite.class,
        BulkTransportTestSuite.class,
        IngestTransportTestSuite.class
})
public class AllTestSuite {
}
