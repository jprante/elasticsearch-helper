package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.helper.ConfigHelperTest;

@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        ConfigHelperTest.class
})
public class MiscTestSuite {
}
