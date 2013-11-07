
package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class RestIngestResponseAction extends BaseRestHandler {

    @Inject
    public RestIngestResponseAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(GET, "/_ingest", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        try {
            XContentBuilder builder = restContentBuilder(request);
            builder.startObject();
            builder.field(Fields.OK, true);
            builder.startArray(Fields.RESPONSES);
            synchronized (RestIngestAction.responses) {
                for (Map.Entry<Long, Object> me : RestIngestAction.responses.entrySet()) {
                    builder.startObject();
                    builder.field(Fields.ID, me.getKey());
                    if (me.getValue() instanceof IngestResponse) {
                        IngestResponse response = (IngestResponse) me.getValue();
                        builder.startObject();
                        builder.field(Fields.TOOK, response.tookInMillis());
                        builder.field(Fields.SUCCESS, response.successSize());
                        builder.startArray(Fields.FAILURE);
                        for (IngestItemFailure failure : response.failure()) {
                            builder.field(Fields.ID, failure.id());
                        }
                        builder.endArray();
                        builder.endObject();
                    } else if (me.getValue() instanceof Throwable) {
                        Throwable t = (Throwable)me.getValue();
                        builder.field(Fields.ERROR, t.getMessage());
                    }
                    builder.endObject();
                }
                RestIngestAction.responses.clear(); // write each response only once
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString RESPONSES = new XContentBuilderString("responses");
        static final XContentBuilderString SUCCESS = new XContentBuilderString("success");
        static final XContentBuilderString FAILURE = new XContentBuilderString("failure");
        static final XContentBuilderString ID = new XContentBuilderString("id");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
    }
}
