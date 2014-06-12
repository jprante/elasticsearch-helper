package org.xbib.json.patch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializable;
import org.xbib.json.pointer.JsonPointer;

import static com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "op")

@JsonSubTypes({
        @Type(name = "add", value = AddOperation.class),
        @Type(name = "copy", value = CopyOperation.class),
        @Type(name = "move", value = MoveOperation.class),
        @Type(name = "remove", value = RemoveOperation.class),
        @Type(name = "replace", value = ReplaceOperation.class),
        @Type(name = "test", value = TestOperation.class)
})

/**
 * Base abstract class for one patch operation
 *
 * <p>Two more abstract classes extend this one according to the arguments of
 * the operation:</p>
 *
 * <ul>
 *     <li>{@link DualPathOperation} for operations taking a second pointer as
 *     an argument ({@code copy} and {@code move});</li>
 *     <li>{@link PathValueOperation} for operations taking a value as an
 *     argument ({@code add}, {@code replace} and {@code test}).</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonPatchOperation
        implements JsonSerializable {
    protected final String op;

    /*
     * Note: no need for a custom deserializer, Jackson will try and find a
     * constructor with a single string argument and use it.
     *
     * However, we need to serialize using .toString().
     */
    protected final JsonPointer path;

    /**
     * Constructor
     *
     * @param op   the operation name
     * @param path the JSON Pointer for this operation
     */
    protected JsonPatchOperation(final String op, final JsonPointer path) {
        this.op = op;
        this.path = path;
    }

    /**
     * Apply this operation to a JSON value
     *
     * @param node the value to patch
     * @return the patched value
     */
    public abstract JsonNode apply(final JsonNode node) throws JsonPatchException;

    @Override
    public abstract String toString();
}
