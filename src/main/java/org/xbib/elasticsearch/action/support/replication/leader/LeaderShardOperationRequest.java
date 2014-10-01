package org.xbib.elasticsearch.action.support.replication.leader;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.support.replication.Consistency;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public abstract class LeaderShardOperationRequest<T extends LeaderShardOperationRequest> extends ActionRequest<T>
        implements IndicesRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    public static final Consistency DEFAULT_CONSISTENCY = Consistency.QUORUM;

    protected TimeValue timeout = DEFAULT_TIMEOUT;

    protected Consistency requiredConsistency = DEFAULT_CONSISTENCY;

    protected String index;

    private boolean threadedOperation = true;

    protected LeaderShardOperationRequest() {
    }

    public LeaderShardOperationRequest(T request) {
        super(request);
        this.timeout = request.timeout();
        this.index = request.index();
        this.requiredConsistency = request.requiredConsistency();
        this.threadedOperation = request.operationThreaded();
    }

    public final boolean operationThreaded() {
        return threadedOperation;
    }

    @SuppressWarnings("unchecked")
    public final T operationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    public final T timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null));
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @SuppressWarnings("unchecked")
    public final T index(String index) {
        this.index = index;
        return (T) this;
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
    public final T requiredConsistency(Consistency requiredConsistency) {
        this.requiredConsistency = requiredConsistency;
        return (T) this;
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        timeout = TimeValue.readTimeValue(in);
        index = in.readString();
        requiredConsistency = Consistency.fromId(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        timeout.writeTo(out);
        out.writeString(index);
        out.writeByte(requiredConsistency.id());
    }

    public void beforeLocalFork() {

    }
}
