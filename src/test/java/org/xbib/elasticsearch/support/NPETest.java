package org.xbib.elasticsearch.support;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.testng.annotations.Test;

public class NPETest extends AbstractNodeTest {

    @Test
    public void testNPE1() throws Exception {
        Client client = client("1");
        BulkRequestBuilder builder = new BulkRequestBuilder(client)
                .add(Requests.indexRequest());
        client.bulk(builder.request()).actionGet();
    }

    @Test
    public void testNPE2() throws Exception {
        Client client = client("1");
        BulkRequestBuilder builder = new BulkRequestBuilder(client)
                .add((IndexRequest)null);
        client.bulk(builder.request()).actionGet();
    }

    @Test
    public void testNPE3() throws Exception {
        Client client = client("1");
        IndexRequestBuilder r = new IndexRequestBuilder(client);
        BulkRequestBuilder builder = new BulkRequestBuilder(client)
                .add(r.request());
        client.bulk(builder.request()).actionGet();
    }

    @Test
    public void testNPE4() throws Exception {
        Client client = client("1");
        client.prepareBulk()
                .add(Requests.indexRequest().index(null).type(null).id(null))
                .execute().actionGet();
    }

}
