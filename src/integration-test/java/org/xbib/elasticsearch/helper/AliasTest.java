
package org.xbib.elasticsearch.helper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AliasTest extends NodeTestUtils {

    private static final ESLogger logger = ESLoggerFactory.getLogger(AliasTest.class.getName());

    @Test
    public void testAlias() throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest("test");
        client("1").admin().indices().create(indexRequest).actionGet();
        // put alias
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        String[] indices = new String[]{"test"};
        String[] aliases = new String[]{"test_alias"};
        IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD, indices, aliases);
        indicesAliasesRequest.addAliasAction(aliasAction);
        client("1").admin().indices().aliases(indicesAliasesRequest).actionGet();
        // get alias
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(Strings.EMPTY_ARRAY);
        long t0 = System.nanoTime();
        GetAliasesResponse getAliasesResponse = client("1").admin().indices().getAliases(getAliasesRequest).actionGet();
        long t1 = (System.nanoTime() - t0) / 1000000;
        logger.info("{} time(ms) = {}", getAliasesResponse.getAliases(), t1);
        assertTrue(t1 >= 0);
    }

    @Test
    public void testMostRecentIndex() throws IOException {
        String alias = "test";
        CreateIndexRequest indexRequest = new CreateIndexRequest("test20160101");
        client("1").admin().indices().create(indexRequest).actionGet();
        indexRequest = new CreateIndexRequest("test20160102");
        client("1").admin().indices().create(indexRequest).actionGet();
        indexRequest = new CreateIndexRequest("test20160103");
        client("1").admin().indices().create(indexRequest).actionGet();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        String[] indices = new String[]{"test20160101","test20160102","test20160103"};
        String[] aliases = new String[]{alias};
        IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD, indices, aliases);
        indicesAliasesRequest.addAliasAction(aliasAction);
        client("1").admin().indices().aliases(indicesAliasesRequest).actionGet();

        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client("1"), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> result = new TreeSet<>(Collections.reverseOrder());
        for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
            Matcher m = pattern.matcher(indexName.value);
            if (m.matches()) {
                if (alias.equals(m.group(1))) {
                    result.add(indexName.value);
                }
            }
        }
        Iterator<String> it = result.iterator();
        assertEquals("test20160103", it.next());
        assertEquals("test20160102", it.next());
        assertEquals("test20160101", it.next());
        logger.info("result={}", result);
    }

}
