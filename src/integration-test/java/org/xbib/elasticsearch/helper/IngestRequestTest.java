package org.xbib.elasticsearch.helper;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.junit.Test;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.IngestRequestBuilder;
import org.xbib.elasticsearch.NodeTestUtils;

public class IngestRequestTest extends NodeTestUtils {

    @Test(expected = ActionRequestValidationException.class)
    public void testIngest1() {
        Client client = client("1");
        IngestRequestBuilder builder = new IngestRequestBuilder(client, IngestAction.INSTANCE)
                .add(Requests.indexRequest());
        client.execute(IngestAction.INSTANCE, builder.request()).actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testIngest2() {
        Client client = client("1");
        IngestRequestBuilder builder = new IngestRequestBuilder(client, IngestAction.INSTANCE)
                .add((IndexRequest)null);
        client.execute(IngestAction.INSTANCE, builder.request()).actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testIngest3() {
        Client client = client("1");
        IndexRequest r = new IndexRequestBuilder(client, IndexAction.INSTANCE).request();
        IngestRequestBuilder builder = new IngestRequestBuilder(client, IngestAction.INSTANCE)
                .add(r);
        client.execute(IngestAction.INSTANCE, builder.request()).actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testIngest4() {
        Client client = client("1");
        IndexRequest r = Requests.indexRequest().index(null).type(null).id(null);
        IngestRequestBuilder builder = new IngestRequestBuilder(client, IngestAction.INSTANCE)
                .add(r);
        client.execute(IngestAction.INSTANCE, builder.request()).actionGet();
    }

}
