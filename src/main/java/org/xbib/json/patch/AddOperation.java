package org.xbib.json.patch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.xbib.json.pointer.JsonPointer;
import org.xbib.json.pointer.ReferenceToken;
import org.xbib.json.pointer.TokenResolver;

/**
 * JSON Patch {@code add} operation
 * <p>
 * <p>For this operation, {@code path} is the JSON Pointer where the value
 * should be added, and {@code value} is the value to add.</p>
 * <p>
 * <p>Note that if the target value pointed to by {@code path} already exists,
 * it is replaced. In this case, {@code add} is equivalent to {@code replace}.
 * </p>
 * <p>
 * <p>Note also that a value will be created at the target path <b>if and only
 * if</b> the immediate parent of that value exists (and is of the correct
 * type).</p>
 * <p>
 * <p>Finally, if the last reference token of the JSON Pointer is {@code -} and
 * the immediate parent is an array, the given value is added at the end of the
 * array. For instance, applying:</p>
 * <p>
 * <pre>
 *     { "op": "add", "path": "/-", "value": 3 }
 * </pre>
 * <p>
 * <p>to:</p>
 * <p>
 * <pre>
 *     [ 1, 2 ]
 * </pre>
 * <p>
 * <p>will give:</p>
 * <p>
 * <pre>
 *     [ 1, 2, 3 ]
 * </pre>
 */
public final class AddOperation extends PathValueOperation {

    private static final ReferenceToken LAST_ARRAY_ELEMENT = ReferenceToken.fromRaw("-");

    @JsonCreator
    public AddOperation(@JsonProperty("path") final JsonPointer path,
                        @JsonProperty("value") final JsonNode value) {
        super("add", path, value);
    }

    @Override
    public JsonNode apply(final JsonNode node) throws JsonPatchException {
        if (path.isEmpty()) {
            return value;
        }

        /*
         * Check the parent node: it must exist and be a container (ie an array
         * or an object) for the add operation to work.
         */
        final JsonNode parentNode = path.parent().path(node);
        if (parentNode.isMissingNode()) {
            throw new JsonPatchException("no such parent");
        }
        if (!parentNode.isContainerNode()) {
            throw new JsonPatchException("parent not container");
        }
        return parentNode.isArray()
                ? addToArray(path, node)
                : addToObject(path, node);
    }

    private JsonNode addToArray(final JsonPointer path, final JsonNode node) throws JsonPatchException {
        final JsonNode ret = node.deepCopy();
        final ArrayNode target = (ArrayNode) path.parent().get(ret);
        final TokenResolver<JsonNode> token = path.getLast();
        if (token.getToken().equals(LAST_ARRAY_ELEMENT)) {
            target.add(value);
            return ret;
        }
        final int size = target.size();
        final int index;
        try {
            index = Integer.parseInt(token.toString());
        } catch (NumberFormatException ignored) {
            throw new JsonPatchException("not an index: " + token.toString());
        }
        if (index < 0 || index > size) {
            throw new JsonPatchException("no such index: " + index);
        }
        target.insert(index, value);
        return ret;
    }

    private JsonNode addToObject(final JsonPointer path, final JsonNode node) {
        final JsonNode ret = node.deepCopy();
        final ObjectNode target = (ObjectNode) path.parent().get(ret);
        target.put(path.getLast().getToken().getRaw(), value);
        return ret;
    }
}
