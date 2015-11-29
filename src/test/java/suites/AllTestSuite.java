package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.helper.ConfigHelperTest;
import org.xbib.elasticsearch.helper.AliasTest;
import org.xbib.elasticsearch.helper.IngestRequestTest;
import org.xbib.elasticsearch.helper.SearchTest;
import org.xbib.elasticsearch.helper.SimpleTest;
import org.xbib.elasticsearch.helper.WildcardTest;

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
