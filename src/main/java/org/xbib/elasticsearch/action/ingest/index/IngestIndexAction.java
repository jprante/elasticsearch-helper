
package org.xbib.elasticsearch.action.ingest.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequestOptions;

import org.xbib.elasticsearch.action.ingest.IngestResponse;

public class IngestIndexAction extends Action<IngestIndexRequest, IngestResponse, IngestIndexRequestBuilder> {

    public static final IngestIndexAction INSTANCE = new IngestIndexAction();

    public static final String NAME = "org.xbib.elasticsearch.action.ingest.index";

    public IngestIndexAction() {
        super(NAME);
    }

    @Override
    public IngestResponse newResponse() {
        return new IngestResponse();
    }

    @Override
    public IngestIndexRequestBuilder newRequestBuilder(Client client) {
        return new IngestIndexRequestBuilder(client);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.options()
                .withType(TransportRequestOptions.Type.fromString(settings.get("action.bulk.transport.type", TransportRequestOptions.Type.LOW.toString())))
                .withTimeout(settings.getAsTime("action.ingest.timeout", TimeValue.timeValueMinutes(1)))
                .withCompress(settings.getAsBoolean("action.ingest.compress", true));
    }
}
