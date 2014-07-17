package org.xbib.elasticsearch.support.various;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.junit.Test;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

public class NPETest extends AbstractNodeTestHelper {

    @Test(expected = NullPointerException.class)
    public void testNPE1() throws NullPointerException {
        Client client = client("1");
        BulkRequestBuilder builder = new BulkRequestBuilder(client)
                .add(Requests.indexRequest());
        client.bulk(builder.request()).actionGet();
    }

    @Test(expected = NullPointerException.class)
    public void testNPE2() {
        Client client = client("1");
        BulkRequestBuilder builder = new BulkRequestBuilder(client)
                .add((IndexRequest)null);
        client.bulk(builder.request()).actionGet();
    }

    @Test(expected = NullPointerException.class)
    public void testNPE3() {
        Client client = client("1");
        IndexRequestBuilder r = new IndexRequestBuilder(client);
        BulkRequestBuilder builder = new BulkRequestBuilder(client)
                .add(r.request());
        client.bulk(builder.request()).actionGet();
    }

    @Test(expected = NullPointerException.class)
    public void testNPE4() {
        Client client = client("1");
        client.prepareBulk()
                .add(Requests.indexRequest().index(null).type(null).id(null))
                .execute().actionGet();
    }

}
