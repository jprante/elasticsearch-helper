package org.xbib.elasticsearch.action.ingest.leader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class IngestLeaderShardResponse extends ActionResponse {

    private long ingestId;

    private ShardId shardId;

    private int quorumShards;

    private int successCount;

    private long tookInMillis;

    private List<ActionRequest<?>> actionRequests = new LinkedList<ActionRequest<?>>();

    private List<IngestActionFailure> failures = Collections.synchronizedList(new LinkedList<IngestActionFailure>());

    public IngestLeaderShardResponse() {
        super();
    }

    public IngestLeaderShardResponse setIngestId(long ingestId) {
        this.ingestId = ingestId;
        return this;
    }

    public long ingestId() {
        return ingestId;
    }

    public IngestLeaderShardResponse setShardId(ShardId shardId) {
        this.shardId = shardId;
        return this;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int getQuorumShards() {
        return quorumShards;
    }

    public IngestLeaderShardResponse setQuorumShards(int quorumShards) {
        this.quorumShards = quorumShards;
        return this;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public IngestLeaderShardResponse setSuccessCount(int successCount) {
        this.successCount = successCount;
        return this;
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public IngestLeaderShardResponse setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
        return this;
    }

    public List<ActionRequest<?>> getActionRequests() {
        return actionRequests;
    }

    public IngestLeaderShardResponse setActionRequests(List<ActionRequest<?>> actionRequests) {
        this.actionRequests = actionRequests;
        return this;
    }

    public List<IngestActionFailure> getFailures() {
        return failures;
    }

    public IngestLeaderShardResponse setFailures(List<IngestActionFailure> failures) {
        this.failures = failures;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readLong();
        ingestId = in.readLong();
        if (in.readBoolean()) {
            shardId = ShardId.readShardId(in);
        }
        successCount = in.readVInt();
        quorumShards = in.readVInt();
        actionRequests = new LinkedList<>();
        int size = in.readVInt();
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
        failures = new LinkedList<>();
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            failures.add(IngestActionFailure.from(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(tookInMillis);
        out.writeLong(ingestId);
        if (shardId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            shardId.writeTo(out);
        }
        out.writeVInt(successCount);
        out.writeVInt(quorumShards);
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
        out.writeVInt(failures.size());
        for (IngestActionFailure f : failures) {
            f.writeTo(out);
        }
    }

    public String toString() {
        return "ingestId=" + ingestId
                + ",shardId=" + shardId
                + ",successCount=" + successCount
                + ",quorumShards=" + quorumShards
                + ",tookInMillis=" + tookInMillis
                + ",failureCount=" + failures.size()
                + ",failures=" + failures;
    }
}