
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.ingest.index.IngestIndexShardRequest;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.collect.Queues.newConcurrentLinkedQueue;

public class IngestDeleteRequest extends ActionRequest {

    private static final int REQUEST_OVERHEAD = 50;

    private final Queue<DeleteRequest> requests = newQueue();

    private final AtomicLong sizeInBytes = new AtomicLong();

    private ReplicationType replicationType = ReplicationType.DEFAULT;

    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    private TimeValue timeout = IngestIndexShardRequest.DEFAULT_TIMEOUT;

    private String defaultIndex;

    private String defaultType;

    public Queue<DeleteRequest> newQueue() {
        return newConcurrentLinkedQueue();
    }

    public IngestDeleteRequest setIndex(String index) {
        this.defaultIndex = index;
        return this;
    }

    public String getIndex() {
        return defaultIndex;
    }

    public IngestDeleteRequest setType(String type) {
        this.defaultType = type;
        return this;
    }

    public String getType() {
        return defaultType;
    }

    public Queue<DeleteRequest> requests() {
        return requests;
    }

    /**
     * Adds a list of delete requests to be executed.
     */
    public IngestDeleteRequest add(DeleteRequest... requests) {
        for (DeleteRequest request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public IngestDeleteRequest add(Iterable<DeleteRequest> requests) {
        for (DeleteRequest request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows
     * the same behavior of {@link org.elasticsearch.action.index.IndexRequest} (for example, if no id is
     * provided, one will be generated, or usage of the create flag).
     */
    public IngestDeleteRequest add(DeleteRequest request) {
        request.beforeLocalFork();
        return internalAdd(request);
    }

    IngestDeleteRequest internalAdd(DeleteRequest request) {
        requests.add(request);
        sizeInBytes.addAndGet(REQUEST_OVERHEAD);
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
    public IngestDeleteRequest consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Set the replication type for this operation.
     */
    public IngestDeleteRequest replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    public ReplicationType replicationType() {
        return this.replicationType;
    }

    /**
     * Set the timeout for this operation.
     */
    public IngestDeleteRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    /**
     * Take all requests out of this bulk request.
     * This method is thread safe.
     *
     * @return another bulk request
     */
    public IngestDeleteRequest takeAll() {
        IngestDeleteRequest request = new IngestDeleteRequest();
        while (!requests.isEmpty()) {
            DeleteRequest deleteRequest = requests.poll();
            if (deleteRequest != null) {
                request.add(deleteRequest);
            }
        }
        return request;
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
    public IngestDeleteRequest take(int numRequests) {
        IngestDeleteRequest request = new IngestDeleteRequest();
        for (int i = 0; i < numRequests; i++) {
            DeleteRequest deleteRequest = requests.poll();
            if (deleteRequest != null) {
                request.add(deleteRequest);
            }
        }
        return request;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            DeleteRequest request = new DeleteRequest();
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
