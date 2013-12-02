
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

import org.xbib.elasticsearch.action.ingest.IngestResponse;

/**
 * A bulk request holds ordered {@link org.elasticsearch.action.delete.DeleteRequest}s
 * and allows to executes it in a single batch.
 */
public class IngestDeleteRequestBuilder extends ActionRequestBuilder<IngestDeleteRequest, IngestResponse, IngestDeleteRequestBuilder> {

    public IngestDeleteRequestBuilder(Client client) {
        super((InternalClient)client, new IngestDeleteRequest());
    }

    public IngestDeleteRequest request() {
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

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public IngestDeleteRequestBuilder add(DeleteRequest request) {
        this.request.add(request);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public IngestDeleteRequestBuilder add(DeleteRequestBuilder request) {
        this.request.add(request.request());
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    public IngestDeleteRequestBuilder setReplicationType(ReplicationType replicationType) {
        this.request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public IngestDeleteRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }

    protected void doExecute(ActionListener<IngestResponse> listener) {
        ((InternalClient)client).execute(IngestDeleteAction.INSTANCE, request, listener);
    }
}
