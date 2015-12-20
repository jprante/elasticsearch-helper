
package org.xbib.elasticsearch.helper;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;

import java.io.IOException;

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

}
