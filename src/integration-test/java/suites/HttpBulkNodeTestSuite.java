package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.helper.client.http.HttpBulkNodeClientTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        HttpBulkNodeClientTest.class
})
public class HttpBulkNodeTestSuite {

}
