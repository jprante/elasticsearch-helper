
package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * Ingest action
 */
public class IngestAction extends Action<IngestRequest, IngestResponse, IngestRequestBuilder> {

    public static final IngestAction INSTANCE = new IngestAction();

    public static final String NAME = "ingest";

    private IngestAction() {
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
                .withType(TransportRequestOptions.Type.fromString(settings.get("action.ingest.transport.type", TransportRequestOptions.Type.LOW.toString())))
                .withCompress(settings.getAsBoolean("action.ingest.compress", true));
    }
}
