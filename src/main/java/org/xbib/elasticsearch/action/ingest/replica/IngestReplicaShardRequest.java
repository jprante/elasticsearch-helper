package org.xbib.elasticsearch.action.ingest.replica;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IngestReplicaShardRequest extends ActionRequest<IngestReplicaShardRequest> {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    private TimeValue timeout = DEFAULT_TIMEOUT;

    private String index;

    private boolean threadedOperation = true;

    private long ingestId;

    private ShardId shardId;

    private List<ActionRequest<?>> actionRequests = new LinkedList<>();

    public IngestReplicaShardRequest() {
    }

    public IngestReplicaShardRequest(long ingestId, ShardId shardId, List<ActionRequest<?>> actionRequests) {
        this.index = shardId.index().name();
        this.ingestId = ingestId;
        this.shardId = shardId;
        this.actionRequests = actionRequests;
    }

    public long ingestId() {
        return ingestId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public List<ActionRequest<?>> actionRequests() {
        return actionRequests;
    }

    public final boolean operationThreaded() {
        return threadedOperation;
    }

    @SuppressWarnings("unchecked")
    public final IngestReplicaShardRequest operationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return this;
    }

    @SuppressWarnings("unchecked")
    public final IngestReplicaShardRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return index;
    }

    @SuppressWarnings("unchecked")
    public final IngestReplicaShardRequest index(String index) {
        this.index = index;
        return this;
    }

    public String[] indices() {
        return new String[]{index};
    }

    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", null);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        timeout.writeTo(out);
        out.writeLong(ingestId);
        shardId.writeTo(out);
        out.writeVInt(actionRequests.size());
        for (ActionRequest<?> actionRequest : actionRequests) {
            if (actionRequest == null) {
                out.writeBoolean(false);
                continue;
            }
            out.writeBoolean(true);
            if (actionRequest instanceof IndexRequest) {
                out.writeBoolean(true);
            } else if (actionRequest instanceof DeleteRequest) {
                out.writeBoolean(false);
            } else {
                throw new ElasticsearchException("action request not supported: " + actionRequest.getClass().getName());
            }
            actionRequest.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        timeout = TimeValue.readTimeValue(in);
        ingestId = in.readLong();
        shardId = ShardId.readShardId(in);
        int size = in.readVInt();
        actionRequests = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            boolean exists = in.readBoolean();
            if (exists) {
                boolean b = in.readBoolean();
                if (b) {
                    IndexRequest indexRequest = new IndexRequest();
                    indexRequest.readFrom(in);
                    actionRequests.add(indexRequest);
                } else {
                    DeleteRequest deleteRequest = new DeleteRequest();
                    deleteRequest.readFrom(in);
                    actionRequests.add(deleteRequest);
                }
            } else {
                actionRequests.add(null);
            }
        }
    }
}
