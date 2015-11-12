package org.xbib.elasticsearch.action.search.helper;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper class for Elasticsearch responses
 */
public class BasicGetResponse {

    private GetResponse getResponse;

    private static byte[] jsonErrorMessage(String message) {
        return ("{\"error\":500,\"message\":\"" + message + "\"}").getBytes();
    }

    public GetResponse getResponse() {
        return getResponse;
    }

    public BasicGetResponse setResponse(GetResponse response) {
        this.getResponse = response;
        return this;
    }

    public boolean exists() {
        return getResponse.isExists();
    }

    public BasicGetResponse toJson(OutputStream out) throws IOException {
        if (out == null) {
            return this;
        }
        if (getResponse == null) {
            out.write(jsonErrorMessage("no response yet"));
            return this;
        }
        XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, out);
        getResponse.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.close();
        return this;
    }


}
