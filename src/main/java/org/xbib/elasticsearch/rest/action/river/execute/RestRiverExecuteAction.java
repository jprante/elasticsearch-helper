package org.xbib.elasticsearch.rest.action.river.execute;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.xbib.elasticsearch.action.river.execute.RiverExecuteAction;
import org.xbib.elasticsearch.action.river.execute.RiverExecuteRequest;
import org.xbib.elasticsearch.action.river.execute.RiverExecuteResponse;
import org.xbib.elasticsearch.rest.action.support.AbstractRestRiverAction;

/**
 * Run a river. The river can be executed once with such a call. Example:
 *
 * curl -XPOST 'localhost:9200/_river/my_jdbc_river/_execute'
 */
public class RestRiverExecuteAction extends AbstractRestRiverAction {

    @Inject
    public RestRiverExecuteAction(Settings settings, Client client, RestController controller, Injector injector) {
        super(settings, client, injector);
        controller.registerHandler(RestRequest.Method.POST, "/_river/{riverType}/{riverName}/_execute", this);
    }

    @Override
    public void handleRequest(final RestRequest request, RestChannel channel) {
        String riverName = request.param("riverName");
        if (riverName == null || riverName.isEmpty()) {
            respond(false, request, channel, "parameter 'riverName' is required", RestStatus.BAD_REQUEST);
            return;
        }
        String riverType = request.param("riverType");
        RiverExecuteRequest riverExecuteRequest = new RiverExecuteRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        RiverExecuteResponse riverExecuteResponse = client
                .execute(RiverExecuteAction.INSTANCE, riverExecuteRequest).actionGet();
        boolean isExecuted = false;
        for (int i = 0; i < riverExecuteResponse.isExecuted().length; i++) {
            isExecuted = isExecuted || riverExecuteResponse.isExecuted()[i];
        }
        respond(isExecuted, request, channel, "ok", RestStatus.OK);
    }

}
