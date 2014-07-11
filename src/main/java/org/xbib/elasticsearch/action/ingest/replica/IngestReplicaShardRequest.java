package org.xbib.elasticsearch.action.ingest.replica;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.action.support.replication.replica.ReplicaShardOperationRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class IngestReplicaShardRequest extends ReplicaShardOperationRequest<IngestReplicaShardRequest> {

    private long ingestId;

    private ShardId shardId;

    private List<ActionRequest> actionRequests = newLinkedList();

    public IngestReplicaShardRequest() {
    }

    public IngestReplicaShardRequest(long ingestId, ShardId shardId, List<ActionRequest> actionRequests) {
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

    public List<ActionRequest> actionRequests() {
        return actionRequests;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeLong(ingestId);
        shardId.writeTo(out);
        out.writeVInt(actionRequests.size());
        for (ActionRequest actionRequest : actionRequests) {
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
                throw new ElasticsearchIllegalStateException("action request not supported: " + actionRequest.getClass().getName());
            }
            actionRequest.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        ingestId = in.readLong();
        shardId = ShardId.readShardId(in);
        int size = in.readVInt();
        actionRequests = newLinkedList();
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
