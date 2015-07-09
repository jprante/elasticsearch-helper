package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

public class IngestIndexCreationTest extends AbstractNodeRandomTestHelper {

    @Test
    public void testIngestCreation() throws Exception {

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .build();

        Map<String,String> mappings = newHashMap();
        mappings.put("typename","{\"properties\":{\"message\":{\"type\":\"string\"}}}");
        final IngestTransportClient ingest = new IngestTransportClient();
        try {
            ingest.init(getSettings());
            ingest.newIndex("test", settings, mappings);
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices("test");
            GetMappingsResponse getMappingsResponse = ingest.client().admin().indices()
                    .getMappings(getMappingsRequest).actionGet();
            MappingMetaData md = getMappingsResponse.getMappings().get("test").get("typename");
            assertEquals("{properties={message={type=string}}}", md.getSourceAsMap().toString());
        } finally {
            ingest.shutdown();
        }
    }

}
