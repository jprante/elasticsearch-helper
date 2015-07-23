package org.xbib.elasticsearch.action.ingest.leader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.xbib.elasticsearch.action.ingest.Consistency;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IngestLeaderShardRequest extends ActionRequest<IngestLeaderShardRequest> implements IndicesRequest {

    private TimeValue timeout = Consistency.DEFAULT_TIMEOUT;

    private Consistency requiredConsistency = Consistency.DEFAULT_CONSISTENCY;

    private String index;

    private boolean threadedOperation = true;

    private long ingestId;

    private ShardId shardId;

    private List<ActionRequest<?>> actionRequests = new LinkedList<ActionRequest<?>>();

    public IngestLeaderShardRequest() {
    }

    public long getIngestId() {
        return ingestId;
    }

    public IngestLeaderShardRequest setIngestId(long ingestId) {
        this.ingestId = ingestId;
        return this;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public IngestLeaderShardRequest setShardId(ShardId shardId) {
        this.index = shardId.index().name();
        this.shardId = shardId;
        return this;
    }

    public List<ActionRequest<?>> getActionRequests() {
        return actionRequests;
    }

    public IngestLeaderShardRequest setActionRequests(List<ActionRequest<?>> actionRequests) {
        this.actionRequests = actionRequests;
        return this;
    }

    public final boolean operationThreaded() {
        return threadedOperation;
    }

    @SuppressWarnings("unchecked")
    public final IngestLeaderShardRequest operationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return this;
    }

    @SuppressWarnings("unchecked")
    public final IngestLeaderShardRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @SuppressWarnings("unchecked")
    public final IngestLeaderShardRequest index(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public Consistency requiredConsistency() {
        return this.requiredConsistency;
    }

    @SuppressWarnings("unchecked")
    public final IngestLeaderShardRequest requiredConsistency(Consistency requiredConsistency) {
        this.requiredConsistency = requiredConsistency;
        return this;
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
        out.writeByte(requiredConsistency.id());
        out.writeLong(ingestId);
        shardId.writeTo(out);
        out.writeVInt(actionRequests.size());
        for (ActionRequest<?> actionRequest : actionRequests) {
            if (actionRequest == null) {
                out.writeBoolean(false);
            } else {
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
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        timeout = TimeValue.readTimeValue(in);
        requiredConsistency = Consistency.fromId(in.readByte());
        ingestId = in.readLong();
        shardId = ShardId.readShardId(in);
        int size = in.readVInt();
        actionRequests = new LinkedList<ActionRequest<?>>();
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
