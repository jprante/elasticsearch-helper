package org.xbib.json.diff;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.xbib.json.jackson.JacksonUtils;

import java.util.List;

final class IndexedJsonArray {
    private final int size;
    private final JsonNode node;

    private int index = 0;

    IndexedJsonArray(final JsonNode node) {
        this.node = node;
        size = node.size();
    }

    IndexedJsonArray(final List<JsonNode> list) {
        final ArrayNode arrayNode = JacksonUtils.nodeFactory().arrayNode();
        arrayNode.addAll(list);
        node = arrayNode;
        size = arrayNode.size();
    }

    int getIndex() {
        return isEmpty() ? -1 : index;
    }

    void shift() {
        index++;
    }

    JsonNode getElement() {
        return node.get(index);
    }

    boolean isEmpty() {
        return index >= size;
    }

    int size() {
        return size;
    }
}
