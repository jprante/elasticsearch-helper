
package org.xbib.elasticsearch.action.ingest.index;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.collect.Lists.newArrayListWithCapacity;
import static org.elasticsearch.common.collect.Queues.newConcurrentLinkedQueue;

public class IngestIndexRequest extends ActionRequest {

    private static final int REQUEST_OVERHEAD = 50;

    private final Queue<IndexRequest> requests = newQueue();

    private final AtomicLong sizeInBytes = new AtomicLong();

    private ReplicationType replicationType = ReplicationType.DEFAULT;

    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    private String defaultIndex;

    private String defaultType;

    public Queue<IndexRequest> newQueue() {
        return newConcurrentLinkedQueue();
    }

    public IngestIndexRequest setIndex(String index) {
        this.defaultIndex = index;
        return this;
    }

    public String getIndex() {
        return defaultIndex;
    }

    public IngestIndexRequest setType(String type) {
        this.defaultType = type;
        return this;
    }

    public String getType() {
        return defaultType;
    }

    public Queue<IndexRequest> requests() {
        return requests;
    }

    public IngestIndexRequest add(Collection<IndexRequest> requests, long sizeInBytes) {
        this.requests.addAll(requests);
        this.sizeInBytes.set(sizeInBytes);
        return this;
    }

    public IngestIndexRequest add(IndexRequest... requests) {
        for (IndexRequest request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public IngestIndexRequest add(Iterable<IndexRequest> requests) {
        for (IndexRequest request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows
     * the same behavior of {@link org.elasticsearch.action.index.IndexRequest} (for example, if no id is
     * provided, one will be generated, or usage of the create flag).
     */
    public IngestIndexRequest add(IndexRequest request) {
        request.beforeLocalFork();
        return internalAdd(request);
    }

    IngestIndexRequest internalAdd(IndexRequest request) {
        requests.add(request);
        sizeInBytes.addAndGet(request.source().length() + REQUEST_OVERHEAD);
        return this;
    }

    /**
     * The number of actions in the bulk request.
     */
    public int numberOfActions() {
        // for ConcurrentLinkedQueue, this call is not O(n), and may not be the size of the current list
        return requests.size();
    }

    /**
     * The estimated size in bytes of the bulk request.
     */
    public long estimatedSizeInBytes() {
        return sizeInBytes.longValue();
    }

    /**
     * Sets the consistency level of write. Defaults to
     * {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public IngestIndexRequest consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Set the replication type for this operation.
     */
    public IngestIndexRequest replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    public ReplicationType replicationType() {
        return this.replicationType;
    }

    /**
     * Take all requests out of this bulk request.
     * This method is thread safe.
     *
     * @return another bulk request
     */
    public IngestIndexRequest takeAll() {
        return take(requests.size());
    }

    /**
     * Take a number of requests out of this bulk request and put them
     * into an array list.
     *
     * This method is thread safe.
     *
     * @param numRequests number of requests
     * @return a partial bulk request
     */
    public IngestIndexRequest take(int numRequests) {
        Collection<IndexRequest> partRequest = newArrayListWithCapacity(numRequests);
        long size = 0L;
        for (int i = 0; i < numRequests; i++) {
            IndexRequest request = requests.poll();
            if (request != null) {
                long l = request.source().length() + REQUEST_OVERHEAD;
                sizeInBytes.addAndGet(-l);
                size += l;
                partRequest.add(request);
            }
        }
        return new IngestIndexRequest().add(partRequest, size);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (ActionRequest request : requests) {
            ActionRequestValidationException ex = request.validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IndexRequest request = new IndexRequest();
            request.readFrom(in);
            requests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        out.writeVInt(requests.size());
        for (ActionRequest request : requests) {
            request.writeTo(out);
        }
    }
}
