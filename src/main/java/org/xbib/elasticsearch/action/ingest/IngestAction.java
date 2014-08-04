package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * The ingest action replaces the bulk action, for using the IngestProcessor
 */
public class IngestAction extends ClientAction<IngestRequest, IngestResponse, IngestRequestBuilder> {

    public static final IngestAction INSTANCE = new IngestAction();

    public static final String NAME = "org.xbib.elasticsearch.action.ingest";

    public IngestAction() {
        super(NAME);
    }

    @Override
    public IngestResponse newResponse() {
        return new IngestResponse();
    }

    @Override
    public IngestRequestBuilder newRequestBuilder(Client client) {
        return new IngestRequestBuilder(client);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.options()
                .withType(TransportRequestOptions.Type.BULK)
                .withTimeout(settings.getAsTime("action.ingest.timeout", TimeValue.timeValueSeconds(60)))
                .withCompress(settings.getAsBoolean("action.ingest.compress", true));
    }
}
