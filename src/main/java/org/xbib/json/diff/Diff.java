package org.xbib.json.diff;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.xbib.json.jackson.JsonNumEquals;
import org.xbib.json.jackson.Wrapper;
import org.xbib.json.pointer.JsonPointer;

import java.util.Arrays;
import java.util.Objects;

/**
 * Difference representation. Captures diff information required to
 * generate JSON patch operations and factorize differences.
 */
final class Diff {

    DiffOperation operation;

    JsonPointer path;

    JsonPointer arrayPath;

    int firstArrayIndex;

    int secondArrayIndex;

    final JsonNode value;

    JsonPointer fromPath;

    Diff pairedDiff;

    boolean firstOfPair;

    static Diff simpleDiff(final DiffOperation operation,
                           final JsonPointer path, final JsonNode value) {
        return new Diff(operation, path, value.deepCopy());
    }

    /*
     * "Stateless" removal of a given node from an array given a base path (the
     * immediate parent of an array) and an array index; as the name suggests,
     * this factory method is called only when a node is removed from the tail
     * of a target array; in other words, the source node has extra elements.
     */
    static Diff tailArrayRemove(final JsonPointer basePath, final int index,
                                final int removeIndex, final JsonNode victim) {
        return new Diff(DiffOperation.REMOVE, basePath, index, removeIndex,
                victim.deepCopy());
    }

    /*
     * FIXME: in both usages of this function, array1 is shifted; but we do not
     * do that here: doing it would hide an essential piece of information to
     * the caller.
     *
     * In other words, there is some embarrassing entanglement here which needs
     * to be understood and "decorrelated".
     */
    static Diff arrayRemove(final JsonPointer basePath,
                            final IndexedJsonArray array1, final IndexedJsonArray array2) {
        return new Diff(DiffOperation.REMOVE, basePath, array1.getIndex(),
                array2.getIndex(), array1.getElement().deepCopy());
    }

    static Diff arrayAdd(final JsonPointer basePath, final JsonNode node) {
        return new Diff(DiffOperation.ADD, basePath, -1, -1, node.deepCopy());
    }

    static Diff arrayInsert(final JsonPointer basePath,
                            final IndexedJsonArray array1, final IndexedJsonArray array2) {
        return new Diff(DiffOperation.ADD, basePath, array1.getIndex(),
                array2.getIndex(), array2.getElement().deepCopy());
    }

    private Diff(final DiffOperation operation, final JsonPointer path,
                 final JsonNode value) {
        this.operation = operation;
        this.path = path;
        this.value = value;
    }

    private Diff(final DiffOperation operation, final JsonPointer arrayPath,
                 final int firstArrayIndex, final int secondArrayIndex,
                 final JsonNode value) {
        this.operation = operation;
        this.arrayPath = arrayPath;
        this.firstArrayIndex = firstArrayIndex;
        this.secondArrayIndex = secondArrayIndex;
        this.value = value;
    }

    JsonNode asJsonPatch() {
        final JsonPointer ptr = arrayPath != null ? getSecondArrayPath()
                : path;
        final ObjectNode patch = operation.newOp(ptr);
            /*
             * A remove only has a path
             */
        if (operation == DiffOperation.REMOVE) {
            return patch;
        }
            /*
             * A move has a "source path" (the "from" member), other defined
             * operations (add and replace) have a value instead.
             */
        if (operation == DiffOperation.MOVE
                || operation == DiffOperation.COPY) {
            patch.put("from", fromPath.toString());
        } else {
            patch.put("value", value);
        }
        return patch;
    }

    JsonPointer getSecondArrayPath() {
        // compute path from array path and index
        if (secondArrayIndex != -1) {
            return arrayPath.append(secondArrayIndex);
        }
        return arrayPath.append("-");
    }


    @Override
    public int hashCode() {
        return hashCode(operation, path, arrayPath, firstArrayIndex,
                secondArrayIndex, new Wrapper(JsonNumEquals.getInstance(), value),
                fromPath, pairedDiff != null, firstOfPair);
    }

    private int hashCode(Object... objects) {
        return Arrays.hashCode(objects);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Diff other = (Diff) obj;
        return operation == other.operation
                && Objects.equals(path, other.path)
                && Objects.equals(arrayPath, other.arrayPath)
                && firstArrayIndex == other.firstArrayIndex
                && secondArrayIndex == other.secondArrayIndex
                && JsonNumEquals.getInstance().equivalent(value, other.value)
                && Objects.equals(fromPath, other.fromPath)
                && Objects.equals(pairedDiff != null, other.pairedDiff != null)
                && firstOfPair == other.firstOfPair;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName())
                .append("{op=").append(operation)
                .append("}{path=").append(path)
                .append("}{arrayPath=").append(arrayPath)
                .append("}{firstArrayIndex=").append(firstArrayIndex)
                .append("}{secondArrayIndex").append(secondArrayIndex)
                .append("}{value").append(value)
                .append("}{fromPath").append(fromPath)
                .append("}{paired").append(pairedDiff != null)
                .append("}{firstOfPair").append(firstOfPair);
        return Objects.toString(sb.toString());
    }
}
