package org.xbib.elasticsearch.support.various;

import java.io.IOException;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.client.Requests.deleteIndexRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class WildcardTest {

    private static final String INDEX = "test";

    private static final String TYPE = "test";

    private static final String FIELD = "field";

    private static Client client;

    public void test() throws Exception {
        initializeClient();
        //deleteIndex();
        index("1", "010");
        index("2", "0*0");
        // exact
        validateCount(queryString("010").defaultField(FIELD), 1);
        validateCount(queryString("0\\*0").defaultField(FIELD), 1);
        // pattern
        validateCount(queryString("0*0").defaultField(FIELD), 2);
        validateCount(queryString("0?0").defaultField(FIELD), 2);
        validateCount(queryString("0**0").defaultField(FIELD), 2);
        validateCount(queryString("0??0").defaultField(FIELD), 0);
        validateCount(queryString("*10").defaultField(FIELD), 1);
        validateCount(queryString("*1*").defaultField(FIELD), 1);
        // failing
        //validateCount(queryString("*\\*0").defaultField(FIELD), 1);
        //validateCount(queryString("*\\**").defaultField(FIELD), 1);
    }

    private void initializeClient() {
        client = nodeBuilder()
                .settings(settingsBuilder()
                .put("cluster.name", "test")
                .put("index.analysis.analyzer.default.filter", "lowercase")
                .put("index.analysis.analyzer.default.tokenizer", "keyword"))
                .local(true).node().client();
    }

    private void deleteIndex() {
        client.admin().indices().delete(deleteIndexRequest(INDEX)).actionGet();
    }

    private void index(String id, String fieldValue) throws IOException {
        client.index(indexRequest()
                .index(INDEX).type(TYPE).id(id)
                .source(jsonBuilder().startObject().field(FIELD, fieldValue).endObject())
                .refresh(true)).actionGet();
    }

    private long count(QueryBuilder queryBuilder) {
        return client.prepareSearch(INDEX).setTypes(TYPE)
                .setQuery(queryBuilder)
                .execute().actionGet().getHits().getTotalHits();
    }

    private void validateCount(QueryBuilder queryBuilder, long expectedHits) {
        final long actualHits = count(queryBuilder);
        if (actualHits != expectedHits) {
            throw new RuntimeException("actualHits=" + actualHits + ", expectedHits=" + expectedHits);
        }
    }
}