package org.xbib.elasticsearch.rest.action.support;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public abstract class AbstractRestRiverAction extends BaseRestHandler {

    protected Injector injector;

    public AbstractRestRiverAction(Settings settings, Client client, Injector injector) {
        super(settings, client);
        this.injector = injector;
    }

    protected void respond(boolean success, RestRequest request, RestChannel channel, String error, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            if (request.paramAsBoolean("pretty", false)) {
                builder.prettyPrint();
            }
            builder.startObject()
                    .field("success", success);
            if (error != null) {
                builder.field("error", error);
            }
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    protected void errorResponse(RestRequest request, RestChannel channel, Throwable e) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
        } catch (IOException e1) {
            logger.error("Failed to send failure response", e1);
        }
    }

}
