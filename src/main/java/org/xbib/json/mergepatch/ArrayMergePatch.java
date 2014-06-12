package org.xbib.json.mergepatch;

import com.fasterxml.jackson.databind.JsonNode;

final class ArrayMergePatch extends JsonMergePatch {

    private final JsonNode content;

    ArrayMergePatch(final JsonNode content) {
        super(content);
        this.content = clearNulls(content);
    }

    @Override
    public JsonNode apply(final JsonNode input) {
        return content;
    }
}
