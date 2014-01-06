
package org.xbib.elasticsearch.action.ingest.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.BaseRequestBuilder;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;

import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.ingest.IngestResponse;

/**
 * A bulk request holds ordered {@link org.xbib.elasticsearch.action.ingest.index.IngestIndexRequest}s
 * and allows to executes it in a single batch.
 */
public class IngestIndexRequestBuilder extends BaseRequestBuilder<IngestIndexRequest, IngestResponse> {

    public IngestIndexRequestBuilder(Client client) {
        super(client, new IngestIndexRequest());
    }

    public IngestIndexRequest request() {
        return this.request;
    }

    @Override
    public ListenableActionFuture<IngestResponse> execute() {
        PlainListenableActionFuture<IngestResponse> future = new PlainListenableActionFuture<IngestResponse>(request.listenerThreaded(), client.threadPool());
        execute(future);
        return future;
    }

    @Override
    public void execute(ActionListener<IngestResponse> listener) {
        doExecute(listener);
    }

    public IngestIndexRequestBuilder setIndex(String index) {
        this.request.setIndex(index);
        return this;
    }

    public IngestIndexRequestBuilder setType(String index) {
        this.request.setIndex(index);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute.
     * Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public IngestIndexRequestBuilder add(IndexRequest request) {
        this.request.add(request);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequestBuilder} to the list of actions to execute.
     * Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public IngestIndexRequestBuilder add(IndexRequestBuilder request) {
        this.request.add(request.request());
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    public IngestIndexRequestBuilder setReplicationType(ReplicationType replicationType) {
        this.request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public IngestIndexRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * Sets the timeout. Defaults to {@link org.xbib.elasticsearch.action.ingest.IngestShardRequest#DEFAULT_TIMEOUT}.
     */
    public IngestIndexRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }

    protected void doExecute(ActionListener<IngestResponse> listener) {
        client.execute(IngestIndexAction.INSTANCE, request, listener);
    }
}
