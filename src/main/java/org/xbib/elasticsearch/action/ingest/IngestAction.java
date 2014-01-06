
package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequestOptions;

public class IngestAction extends Action<IngestRequest, IngestResponse, IngestRequestBuilder> {

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
                .withType(TransportRequestOptions.Type.fromString(settings.get("action.bulk.transport.type", TransportRequestOptions.Type.LOW.toString())))
                .withTimeout(settings.getAsTime("action.ingest.timeout", TimeValue.timeValueMinutes(1)))
                .withCompress(settings.getAsBoolean("action.ingest.compress", true));
    }
}
