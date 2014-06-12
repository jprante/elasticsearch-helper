package org.xbib.json.mergepatch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.xbib.json.jackson.JacksonUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of <a href="http://tools.ietf.org/html/draft-ietf-appsawg-json-merge-patch-02">JSON merge patch</a>
 * <p>
 * <p>Unlike JSON Patch, JSON Merge Patch only applies to JSON Objects or JSON
 * arrays.</p>
 */
@JsonDeserialize(using = JsonMergePatchDeserializer.class)
public abstract class JsonMergePatch implements JsonSerializable {

    protected static final JsonNodeFactory FACTORY = JacksonUtils.nodeFactory();

    protected final JsonNode origPatch;

    /**
     * Protected constructor
     * <p>
     * <p>Only necessary for serialization purposes. The patching process
     * itself never requires the full node to operate.</p>
     *
     * @param node the original patch node
     */
    protected JsonMergePatch(final JsonNode node) {
        origPatch = node;
    }

    public abstract JsonNode apply(final JsonNode input);

    public static JsonMergePatch fromJson(final JsonNode input) {
        return input.isArray() ? new ArrayMergePatch(input)
                : new ObjectMergePatch(input);
    }

    /**
     * Clear "null values" from a JSON value
     * <p>
     * <p>Non container values are unchanged. For arrays, null elements are
     * removed. From objects, members whose values are null are removed.</p>
     * <p>
     * <p>This method is recursive, therefore arrays within objects, or objects
     * within arrays, or arrays within arrays etc are also affected.</p>
     *
     * @param node the original JSON value
     * @return a JSON value without null values (see description)
     */
    protected static JsonNode clearNulls(final JsonNode node) {
        if (!node.isContainerNode()) {
            return node;
        }

        return node.isArray() ? clearNullsFromArray(node)
                : clearNullsFromObject(node);
    }

    private static JsonNode clearNullsFromArray(final JsonNode node) {
        final ArrayNode ret = FACTORY.arrayNode();

        /*
         * Cycle through array elements. If the element is a null node itself,
         * skip it. Otherwise, add a "cleaned up" element to the result.
         */
        for (final JsonNode element : node) {
            if (!element.isNull()) {
                ret.add(clearNulls(element));
            }
        }

        return ret;
    }

    private static JsonNode clearNullsFromObject(final JsonNode node) {
        final ObjectNode ret = FACTORY.objectNode();
        final Iterator<Map.Entry<String, JsonNode>> iterator
                = node.fields();

        Map.Entry<String, JsonNode> entry;
        JsonNode value;

        /*
         * When faces with an object, cycle through this object's entries.
         *
         * If the value of the entry is a JSON null, don't include it in the
         * result. If not, include a "cleaned up" value for this key instead of
         * the original element.
         */
        while (iterator.hasNext()) {
            entry = iterator.next();
            value = entry.getValue();
            if (!value.isNull()) {
                ret.put(entry.getKey(), clearNulls(value));
            }
        }

        return ret;
    }

    @Override
    public final void serialize(final JsonGenerator jgen,
                                final SerializerProvider provider)
            throws IOException {
        jgen.writeTree(origPatch);
    }

    @Override
    public final void serializeWithType(final JsonGenerator jgen,
                                        final SerializerProvider provider, final TypeSerializer typeSer)
            throws IOException {
        serialize(jgen, provider);
    }
}
