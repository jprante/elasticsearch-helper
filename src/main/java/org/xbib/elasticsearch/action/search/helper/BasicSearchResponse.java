package org.xbib.elasticsearch.action.search.helper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper class for Elasticsearch responses
 */
public class BasicSearchResponse {

    private SearchResponse searchResponse;

    private static byte[] jsonErrorMessage(String message) {
        return ("{\"error\":500,\"message\":\"" + message + "\"}").getBytes();
    }

    public SearchResponse getResponse() {
        return searchResponse;
    }

    public BasicSearchResponse setResponse(SearchResponse response) {
        this.searchResponse = response;
        return this;
    }

    public long tookInMillis() {
        return searchResponse.getTookInMillis();
    }

    public long totalHits() {
        return searchResponse.getHits().getTotalHits();
    }

    public BasicSearchResponse toJson(OutputStream out) throws IOException {
        if (out == null) {
            return this;
        }
        if (searchResponse == null) {
            out.write(jsonErrorMessage("no response"));
            return this;
        }
        XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, out);
        jsonBuilder.startObject();
        searchResponse.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.endObject();
        jsonBuilder.close();
        return this;
    }


}
