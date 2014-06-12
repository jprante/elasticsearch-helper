package org.xbib.json.patch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.xbib.json.pointer.JsonPointer;

import java.io.IOException;

/**
 * Base class for JSON Patch operations taking two JSON Pointers as arguments
 */
public abstract class DualPathOperation extends JsonPatchOperation {
    @JsonSerialize(using = ToStringSerializer.class)
    protected final JsonPointer from;

    /**
     * Protected constructor
     *
     * @param op   operation name
     * @param from source path
     * @param path destination path
     */
    protected DualPathOperation(final String op, final JsonPointer from,
                                final JsonPointer path) {
        super(op, path);
        this.from = from;
    }

    @Override
    public final void serialize(final JsonGenerator jgen,
                                final SerializerProvider provider)
            throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeStringField("op", op);
        jgen.writeStringField("path", path.toString());
        jgen.writeStringField("from", from.toString());
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
        return "op: " + op + "; from: \"" + from + "\"; path: \"" + path + '"';
    }
}
