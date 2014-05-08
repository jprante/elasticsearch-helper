package org.xbib.elasticsearch.rest.action.river.state;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.action.river.state.RiverStateAction;
import org.xbib.elasticsearch.action.river.state.RiverStateRequest;
import org.xbib.elasticsearch.action.river.state.RiverStateResponse;
import org.xbib.elasticsearch.rest.action.support.AbstractRestRiverAction;
import org.xbib.elasticsearch.rest.action.support.RestXContentBuilder;
import org.xbib.elasticsearch.rest.action.support.XContentRestResponse;
import org.xbib.elasticsearch.rest.action.support.XContentThrowableRestResponse;

import java.io.IOException;

public class RestRiverStateAction extends AbstractRestRiverAction {

    @Inject
    public RestRiverStateAction(Settings settings, Client client, RestController controller, Injector injector) {
        super(settings, client, injector);
        controller.registerHandler(RestRequest.Method.GET, "/_river/{riverType}/{riverName}/_state", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        try {
            String riverName = request.param("riverName");
            String riverType = request.param("riverType");
            RiverStateRequest riverStateRequest = new RiverStateRequest()
                    .setRiverName(riverName)
                    .setRiverType(riverType);
            RiverStateResponse riverStateResponse = client
                    .execute(RiverStateAction.INSTANCE, riverStateRequest).actionGet();
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.startArray("state");
            for (RiverState state : riverStateResponse.getStates()) {
                state.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endArray().endObject();
            channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
        } catch (IOException ioe) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ioe));
            } catch (IOException e) {
                logger.error("unable to send response to client");
            }
        }
    }

}
