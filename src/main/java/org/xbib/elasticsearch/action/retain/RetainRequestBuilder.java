package org.xbib.elasticsearch.action.retain;

import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A builder for {@link RetainRequest}.
 */
public class RetainRequestBuilder extends SingleShardOperationRequestBuilder<RetainRequest, RetainResponse, RetainRequestBuilder> {

    RetainRequestBuilder(ElasticsearchClient client, RetainAction action) {
        super(client, action, new RetainRequest());
    }

    public RetainRequestBuilder(ElasticsearchClient client, RetainAction action, String index) {
        super(client, action, new RetainRequest().index(index));
    }

    /**
     * Sets the delta
     */
    public RetainRequestBuilder setDelta(int delta) {
        request().delta(delta);
        return this;
    }

    /**
     * Set min to keep.
     */
    public RetainRequestBuilder setMinToKeep(int minToKeep) {
        request().minToKeep(minToKeep);
        return this;
    }
}
