
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequestOptions;

import org.xbib.elasticsearch.action.ingest.IngestResponse;

public class IngestDeleteAction extends Action<IngestDeleteRequest, IngestResponse, IngestDeleteRequestBuilder> {

    public static final IngestDeleteAction INSTANCE = new IngestDeleteAction();

    public static final String NAME = "org.xbib.elasticsearch.action.ingest.delete";

    private IngestDeleteAction() {
        super(NAME);
    }

    @Override
    public IngestResponse newResponse() {
        return new IngestResponse();
    }

    @Override
    public IngestDeleteRequestBuilder newRequestBuilder(Client client) {
        return new IngestDeleteRequestBuilder(client);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.options()
                .withType(TransportRequestOptions.Type.BULK)
                .withTimeout(settings.getAsTime("action.ingest.timeout", TimeValue.timeValueMinutes(1)))
                .withCompress(settings.getAsBoolean("action.ingest.compress", true));
    }
}
