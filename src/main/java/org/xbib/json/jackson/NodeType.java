package org.xbib.json.jackson;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration for the different types of JSON instances which can be
 * encountered.
 * <p>
 * <p>In addition to what the JSON RFC defines, JSON Schema has an {@code
 * integer} type, which is a numeric value without any fraction or exponent
 * part.</p>
 * <p>
 * <p><b>NOTE:</b> will disappear in Jackson 2.2.x, which has an equivalent
 * enumeration</p>
 */

public enum NodeType {
    /**
     * Array nodes
     */
    ARRAY("array"),
    /**
     * Boolean nodes
     */
    BOOLEAN("boolean"),
    /**
     * Integer nodes
     */
    INTEGER("integer"),
    /**
     * Number nodes (ie, decimal numbers)
     */
    NULL("null"),
    /**
     * Object nodes
     */
    NUMBER("number"),
    /**
     * Null nodes
     */
    OBJECT("object"),
    /**
     * String nodes
     */
    STRING("string");

    /**
     * The name for this type, as encountered in a JSON schema
     */
    private final String name;

    /**
     * Reverse map to find a node type out of this type's name
     */
    private static final Map<String, NodeType> NAME_MAP
            = new HashMap<String, NodeType>();

    /**
     * Mapping of {@link com.fasterxml.jackson.core.JsonToken} back to node types (used in {@link
     * #getNodeType(com.fasterxml.jackson.databind.JsonNode)})
     */
    private static final Map<JsonToken, NodeType> TOKEN_MAP
            = new EnumMap<JsonToken, NodeType>(JsonToken.class);

    static {
        TOKEN_MAP.put(JsonToken.START_ARRAY, ARRAY);
        TOKEN_MAP.put(JsonToken.VALUE_TRUE, BOOLEAN);
        TOKEN_MAP.put(JsonToken.VALUE_FALSE, BOOLEAN);
        TOKEN_MAP.put(JsonToken.VALUE_NUMBER_INT, INTEGER);
        TOKEN_MAP.put(JsonToken.VALUE_NUMBER_FLOAT, NUMBER);
        TOKEN_MAP.put(JsonToken.VALUE_NULL, NULL);
        TOKEN_MAP.put(JsonToken.START_OBJECT, OBJECT);
        TOKEN_MAP.put(JsonToken.VALUE_STRING, STRING);

        for (final NodeType type : NodeType.values()) {
            NAME_MAP.put(type.name, type);
        }
    }

    NodeType(final String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Given a type name, return the corresponding node type
     *
     * @param name the type name
     * @return the node type, or null if not found
     */
    public static NodeType fromName(final String name) {
        return NAME_MAP.get(name);
    }

    /**
     * Given a {@link com.fasterxml.jackson.databind.JsonNode} as an argument, return its type. The argument
     * MUST NOT BE NULL, and MUST NOT be a {@link com.fasterxml.jackson.databind.node.MissingNode}
     *
     * @param node the node to determine the type of
     * @return the type for this node
     */
    public static NodeType getNodeType(final JsonNode node) {
        final JsonToken token = node.asToken();
        final NodeType ret = TOKEN_MAP.get(token);
        return ret;
    }
}
