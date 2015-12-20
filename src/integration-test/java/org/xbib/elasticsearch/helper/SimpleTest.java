package org.xbib.elasticsearch.helper;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.junit.Assert.assertEquals;

public class SimpleTest extends NodeTestUtils {

    protected Settings getNodeSettings() {
        return settingsBuilder()
                .put("path.home", System.getProperty("path.home"))
                .put("index.analysis.analyzer.default.filter.0", "lowercase")
                .put("index.analysis.analyzer.default.filter.1", "trim")
                .put("index.analysis.analyzer.default.tokenizer", "keyword")
                .build();
    }

    @Test
    public void test() throws Exception {
        try {
            DeleteIndexRequestBuilder deleteIndexRequestBuilder = new DeleteIndexRequestBuilder(client("1"), DeleteIndexAction.INSTANCE, "test");
            deleteIndexRequestBuilder.execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(client("1"), IndexAction.INSTANCE);
        indexRequestBuilder
                .setIndex("test")
                .setType("test")
                .setId("1")
                .setSource(jsonBuilder().startObject().field("field", "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8").endObject())
                .setRefresh(true)
                .execute()
                .actionGet();
        String doc = client("1").prepareSearch("test")
                .setTypes("test")
                .setQuery(matchQuery("field", "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8"))
                .execute()
                .actionGet()
                .getHits().getAt(0).getSourceAsString();

        assertEquals(doc, "{\"field\":\"1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8\"}");
    }
}