package org.xbib.elasticsearch.action.ingest.leader;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.xbib.elasticsearch.action.support.replication.leader.LeaderShardOperationRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class IngestLeaderShardRequest extends LeaderShardOperationRequest<IngestLeaderShardRequest> {

    private long ingestId;

    private ShardId shardId;

    private List<ActionRequest> actionRequests = newLinkedList();

    public IngestLeaderShardRequest() {
    }

    public IngestLeaderShardRequest setIngestId(long ingestId) {
        this.ingestId = ingestId;
        return this;
    }

    public long getIngestId() {
        return ingestId;
    }

    public IngestLeaderShardRequest setShardId(ShardId shardId) {
        this.index = shardId.index().name();
        this.shardId = shardId;
        return this;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public IngestLeaderShardRequest setActionRequests(List<ActionRequest> actionRequests) {
        this.actionRequests =  actionRequests;
        return this;
    }

    public List<ActionRequest> getActionRequests() {
        return actionRequests;
    }

    @Override
    public void beforeLocalFork() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(ingestId);
        shardId.writeTo(out);
        out.writeVInt(actionRequests.size());
        for (ActionRequest actionRequest : actionRequests) {
            if (actionRequest == null) {
                out.writeBoolean(false);
            } else {
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
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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
