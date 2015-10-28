package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.action.ingest.leader.IngestLeaderShardResponse;
import org.xbib.elasticsearch.action.ingest.replica.IngestReplicaShardResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class IngestResponse extends ActionResponse implements Iterable<IngestReplicaShardResponse> {

    protected long ingestId;

    protected int successSize;

    protected IngestLeaderShardResponse leaderResponse = new IngestLeaderShardResponse();

    protected List<IngestReplicaShardResponse> replicaResponses = Collections.synchronizedList(new LinkedList<IngestReplicaShardResponse>());

    protected List<IngestActionFailure> failures = Collections.synchronizedList(new LinkedList<IngestActionFailure>());

    protected long tookInMillis;

    public IngestResponse() {
    }

    public IngestResponse setIngestId(long ingestId) {
        this.ingestId = ingestId;
        return this;
    }

    public long ingestId() {
        return ingestId;
    }

    public IngestResponse setSuccessSize(int successSize) {
        this.successSize = successSize;
        return this;
    }

    public int successSize() {
        return successSize;
    }

    public IngestResponse setLeaderResponse(IngestLeaderShardResponse leaderResponse) {
        this.leaderResponse = leaderResponse;
        return this;
    }

    public IngestLeaderShardResponse leaderShardResponse() {
        return leaderResponse;
    }

    public IngestResponse addReplicaResponses(List<IngestReplicaShardResponse> response) {
        this.replicaResponses.addAll(response);
        return this;
    }

    public List<IngestReplicaShardResponse> replicaShardResponses() {
        return replicaResponses;
    }

    public IngestResponse addFailure(IngestActionFailure failure) {
        this.failures.add(failure);
        return this;
    }

    public List<IngestActionFailure> getFailures() {
        return failures;
    }

    public IngestResponse setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
        return this;
    }

    public long tookInMillis() {
        return tookInMillis;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        ingestId = in.readLong();
        successSize = in.readVInt();
        failures = new LinkedList<>();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            failures.add(IngestActionFailure.from(in));
        }
        tookInMillis = in.readVLong();
        leaderResponse = new IngestLeaderShardResponse();
        leaderResponse.readFrom(in);
        replicaResponses = new LinkedList<>();
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IngestReplicaShardResponse r = new IngestReplicaShardResponse();
            r.readFrom(in);
            replicaResponses.add(r);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(ingestId);
        out.writeVInt(successSize);
        out.writeVInt(failures.size());
        for (IngestActionFailure f : failures) {
            f.writeTo(out);
        }
        out.writeVLong(tookInMillis);
        leaderResponse.writeTo(out);
        out.writeVInt(replicaResponses.size());
        for (IngestReplicaShardResponse r : replicaResponses) {
            r.writeTo(out);
        }
    }

    @Override
    public Iterator<IngestReplicaShardResponse> iterator() {
        return replicaResponses.iterator();
    }
}
