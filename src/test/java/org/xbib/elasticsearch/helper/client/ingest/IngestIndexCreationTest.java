package org.xbib.elasticsearch.helper.client.ingest;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class IngestIndexCreationTest extends AbstractNodeRandomTestHelper {

    @Test
    public void testIngestCreation() throws Exception {
        Settings settingsForIndex = Settings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .build();
        Map<String,String> mappings = new HashMap<>();
        mappings.put("typename","{\"properties\":{\"message\":{\"type\":\"string\"}}}");
        final IngestTransportClient ingest = new IngestTransportClient();
        try {
            ingest.init(getSettings(), new LongAdderIngestMetric())
                    .newIndex("test", settingsForIndex, mappings);
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices("test");
            GetMappingsResponse getMappingsResponse =
                    ingest.client().execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
            MappingMetaData md = getMappingsResponse.getMappings().get("test").get("typename");
            assertEquals("{properties={message={type=string}}}", md.getSourceAsMap().toString());
        } finally {
            ingest.shutdown();
        }
    }

}
