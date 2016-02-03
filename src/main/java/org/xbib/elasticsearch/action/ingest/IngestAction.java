package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * The ingest action replaces the bulk action, for using the IngestProcessor
 */
public class IngestAction extends Action<IngestRequest, IngestResponse, IngestRequestBuilder> {

    public static final IngestAction INSTANCE = new IngestAction();

    public static final String NAME = "indices:data/write/xbib/ingest";

    public IngestAction() {
        super(NAME);
    }

    @Override
    public IngestResponse newResponse() {
        return new IngestResponse();
    }

    @Override
    public IngestRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new IngestRequestBuilder(client, INSTANCE);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.BULK)
                .withTimeout(settings.getAsTime("action.ingest.timeout", TimeValue.timeValueSeconds(60)))
                .withCompress(settings.getAsBoolean("action.ingest.compress", true))
                .build();
    }
}
