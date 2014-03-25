package org.xbib.elasticsearch.support.config;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ConfigHelperTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(ConfigHelperTest.class.getName());

    @Test
    public void testConfigHelper() throws IOException {
        ConfigHelper configHelper = new ConfigHelper();
        configHelper.setting(ConfigHelper.class.getResourceAsStream("setting.json"));
        configHelper.setting("index.number_of_shards", 3);
        assertEquals(configHelper.settings().getAsMap().toString(), "{index.analysis.analyzer.default.type=keyword, index.number_of_shards=3}");
     }
}
