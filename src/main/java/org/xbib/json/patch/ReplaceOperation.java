package org.xbib.json.patch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.xbib.json.pointer.JsonPointer;

/**
 * JSON Patch {@code replace} operation
 * <p>
 * <p>For this operation, {@code path} points to the value to replace, and
 * {@code value} is the replacement value.</p>
 * <p>
 * <p>It is an error condition if {@code path} does not point to an actual JSON
 * value.</p>
 */
public final class ReplaceOperation
        extends PathValueOperation {
    @JsonCreator
    public ReplaceOperation(@JsonProperty("path") final JsonPointer path,
                            @JsonProperty("value") final JsonNode value) {
        super("replace", path, value);
    }

    @Override
    public JsonNode apply(final JsonNode node)
            throws JsonPatchException {
        /*
         * FIXME cannot quite be replaced by a remove + add because of arrays.
         * For instance:
         *
         * { "op": "replace", "path": "/0", "value": 1 }
         *
         * with
         *
         * [ "x" ]
         *
         * If remove is done first, the array is empty and add rightly complains
         * that there is no such index in the array.
         */
        if (path.path(node).isMissingNode()) {
            throw new JsonPatchException("no such path");
        }
        final JsonNode replacement = value.deepCopy();
        if (path.isEmpty()) {
            return replacement;
        }
        final JsonNode ret = node.deepCopy();
        final JsonNode parent = path.parent().get(ret);
        final String rawToken = path.getLast().getToken().getRaw();
        if (parent.isObject()) {
            ((ObjectNode) parent).put(rawToken, replacement);
        } else {
            ((ArrayNode) parent).set(Integer.parseInt(rawToken), replacement);
        }
        return ret;
    }
}
