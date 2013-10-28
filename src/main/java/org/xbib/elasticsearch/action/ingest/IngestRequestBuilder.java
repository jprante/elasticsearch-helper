
package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Nullable;

/**
 * A bulk request holds an ordered {@link org.elasticsearch.action.index.IndexRequest}s
 * and {@link org.elasticsearch.action.delete.DeleteRequest}s and allows to executes
 * it in a single batch.
 */
public class IngestRequestBuilder extends ActionRequestBuilder<IngestRequest, IngestResponse, IngestRequestBuilder> {

    protected IngestRequestBuilder(Client client) {
        super((InternalClient) client, new IngestRequest());
    }

    public IngestRequest request() {
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
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public IngestRequestBuilder add(IndexRequest request) {
        this.request.add(request);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public IngestRequestBuilder add(IndexRequestBuilder request) {
        this.request.add(request.request());
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public IngestRequestBuilder add(DeleteRequest request) {
        this.request.add(request);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public IngestRequestBuilder add(DeleteRequestBuilder request) {
        this.request.add(request.request());
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public IngestRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe) throws Exception {
        this.request.add(data, from, length, contentUnsafe, null, null);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public IngestRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        this.request.add(data, from, length, contentUnsafe, defaultIndex, defaultType);
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    public IngestRequestBuilder setReplicationType(ReplicationType replicationType) {
        this.request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public IngestRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
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
        ((InternalClient)client).execute(IngestAction.INSTANCE, request, listener);
    }
}
