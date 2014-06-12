package org.xbib.json.patch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import org.xbib.json.pointer.JsonPointer;

import java.io.IOException;

/**
 * Base class for patch operations taking a value in addition to a path
 */
public abstract class PathValueOperation
        extends JsonPatchOperation {
    @JsonSerialize
    protected final JsonNode value;

    /**
     * Protected constructor
     *
     * @param op    operation name
     * @param path  affected path
     * @param value JSON value
     */
    protected PathValueOperation(final String op, final JsonPointer path,
                                 final JsonNode value) {
        super(op, path);
        this.value = value.deepCopy();
    }

    @Override
    public final void serialize(final JsonGenerator jgen,
                                final SerializerProvider provider)
            throws IOException {
        jgen.writeStartObject();
        jgen.writeStringField("op", op);
        jgen.writeStringField("path", path.toString());
        jgen.writeFieldName("value");
        jgen.writeTree(value);
        jgen.writeEndObject();
    }

    @Override
    public final void serializeWithType(final JsonGenerator jgen,
                                        final SerializerProvider provider, final TypeSerializer typeSer)
            throws IOException, JsonProcessingException {
        serialize(jgen, provider);
    }

    @Override
    public final String toString() {
        return "op: " + op + "; path: \"" + path + "\"; value: " + value;
    }
}
