package org.xbib.elasticsearch.action.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.xbib.elasticsearch.action.support.replication.leader.LeaderShardOperationRequest;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteRequest extends LeaderShardOperationRequest<DeleteRequest> {

    private String type;
    private String id;
    @Nullable
    private String routing;
    private boolean refresh;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;

    public DeleteRequest() {
    }

    public DeleteRequest(String index) {
        this.index = index;
    }

    public DeleteRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public DeleteRequest(DeleteRequest request) {
        super(request);
        this.type = request.type();
        this.id = request.id();
        this.routing = request.routing();
        this.refresh = request.refresh();
        this.version = request.version();
        this.versionType = request.versionType();
    }

    public DeleteRequest(org.elasticsearch.action.delete.DeleteRequest indexRequest) {
        this.index = indexRequest.index();
        this.type = indexRequest.type();
        this.id = indexRequest.id();
        this.routing = indexRequest.routing();
        this.version = indexRequest.version();
        this.versionType = indexRequest.versionType();
        this.refresh = indexRequest.refresh();
    }
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (!versionType.validateVersionForWrites(version)) {
            validationException = addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]", validationException);
        }
        return validationException;
    }

    public String type() {
        return type;
    }

    public DeleteRequest type(String type) {
        this.type = type;
        return this;
    }

    public String id() {
        return id;
    }

    public DeleteRequest id(String id) {
        this.id = id;
        return this;
    }

    public DeleteRequest parent(String parent) {
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    public DeleteRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    public String routing() {
        return this.routing;
    }

    public DeleteRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public DeleteRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    public DeleteRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        refresh = in.readBoolean();
        version = Versions.readVersion(in);
        versionType = VersionType.fromValue(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing());
        out.writeBoolean(refresh);
        Versions.writeVersion(version, out);
        out.writeByte(versionType.getValue());
    }

    @Override
    public String toString() {
        return "delete {[" + index + "][" + type + "][" + id + "]}";
    }
}
