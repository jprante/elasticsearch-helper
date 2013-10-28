
package org.xbib.elasticsearch.action.search.support;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper class for Elasticsearch responses
 */
public class BasicResponse {

    private SearchResponse searchResponse;

    private GetResponse getResponse;

    public BasicResponse searchResponse(SearchResponse response) {
        this.searchResponse = response;
        return this;
    }

    public BasicResponse getResponse(GetResponse response) {
        this.getResponse = response;
        return this;
    }

    public long tookInMillis() {
        return searchResponse.getTookInMillis();
    }

    public long totalHits() {
        return searchResponse.getHits().getTotalHits();
    }

    public boolean exists() {
        return getResponse.isExists();
    }

    public BasicResponse toJson(OutputStream out) throws IOException {
        if (out == null) {
            return this;
        }
        if (searchResponse == null) {
            out.write(jsonErrorMessage("no response yet"));
            return this;
        }
        XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, out);
        jsonBuilder.startObject();
        searchResponse.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.endObject();
        jsonBuilder.close();
        return this;
    }

    private static byte[] jsonEmptyMessage(String message) {
        return ("{\"error\":404,\"message\":\"" + message + "\"}").getBytes();
    }


    private static byte[] jsonErrorMessage(String message) {
        return ("{\"error\":500,\"message\":\"" + message + "\"}").getBytes();
    }


}
