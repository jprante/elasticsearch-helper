
package org.xbib.elasticsearch.action.ingest.create;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

import org.xbib.elasticsearch.action.ingest.IngestResponse;

public class IngestCreateAction extends Action<IngestCreateRequest, IngestResponse, IngestCreateRequestBuilder> {

    public static final IngestCreateAction INSTANCE = new IngestCreateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.ingest.create";

    public IngestCreateAction() {
        super(NAME);
    }

    @Override
    public IngestResponse newResponse() {
        return new IngestResponse();
    }

    @Override
    public IngestCreateRequestBuilder newRequestBuilder(Client client) {
        return new IngestCreateRequestBuilder(client);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.options()
                .withType(TransportRequestOptions.Type.fromString(settings.get("action.ingest.transport.type",
                        TransportRequestOptions.Type.BULK.toString())))
                .withCompress(settings.getAsBoolean("action.ingest.compress", true));
    }
}
