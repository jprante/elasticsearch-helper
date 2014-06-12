package org.xbib.json.patch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.xbib.json.pointer.JsonPointer;

import java.io.IOException;

/**
 * JSON Path {@code remove} operation
 * <p>
 * <p>This operation only takes one pointer ({@code path}) as an argument. It
 * is an error condition if no JSON value exists at that pointer.</p>
 */
public final class RemoveOperation
        extends JsonPatchOperation {
    @JsonCreator
    public RemoveOperation(@JsonProperty("path") final JsonPointer path) {
        super("remove", path);
    }

    @Override
    public JsonNode apply(final JsonNode node)
            throws JsonPatchException {
        if (path.isEmpty()) {
            return MissingNode.getInstance();
        }
        if (path.path(node).isMissingNode()) {
            throw new JsonPatchException("no such path");
        }
        final JsonNode ret = node.deepCopy();
        final JsonNode parentNode = path.parent().get(ret);
        final String raw = path.getLast().getToken().getRaw();
        if (parentNode.isObject()) {
            ((ObjectNode) parentNode).remove(raw);
        } else {
            ((ArrayNode) parentNode).remove(Integer.parseInt(raw));
        }
        return ret;
    }

    @Override
    public void serialize(final JsonGenerator jgen,
                          final SerializerProvider provider)
            throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeStringField("op", "remove");
        jgen.writeStringField("path", path.toString());
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(final JsonGenerator jgen,
                                  final SerializerProvider provider, final TypeSerializer typeSer)
            throws IOException, JsonProcessingException {
        serialize(jgen, provider);
    }

    @Override
    public String toString() {
        return "op: " + op + "; path: \"" + path + '"';
    }
}
