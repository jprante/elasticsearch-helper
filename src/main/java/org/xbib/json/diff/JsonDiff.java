package org.xbib.json.diff;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.xbib.json.jackson.Equivalence;
import org.xbib.json.jackson.JacksonUtils;
import org.xbib.json.jackson.JsonNumEquals;
import org.xbib.json.jackson.NodeType;
import org.xbib.json.pointer.JsonPointer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * "Reverse" factorizing JSON Patch implementation
 * <p>
 * <p>This class only has one method, {@link #asJson(com.fasterxml.jackson.databind.JsonNode, com.fasterxml.jackson.databind.JsonNode)}, which
 * takes two JSON values as arguments and returns a patch as a {@link com.fasterxml.jackson.databind.JsonNode}.
 * This generated patch can then be used in {@link
 * org.xbib.json.patch.JsonPatch#fromJson(com.fasterxml.jackson.databind.JsonNode)}.</p>
 * <p>
 * <p>Numeric equivalence is respected. Operations are always generated in the
 * following order:</p>
 * <p>
 * <ul>
 * <li>additions,</li>
 * <li>removals,</li>
 * <li>replacements.</li>
 * </ul>
 * <p>
 * <p>Array values generate operations in the order of elements. Factorizing is
 * done to merge add and remove into move operations and convert duplicate add
 * to copy operations if values are equivalent. No test operations are
 * generated (they don't really make sense for diffs anyway).</p>
 * <p>
 * <p>Note that due to the way {@link com.fasterxml.jackson.databind.JsonNode} is implemented, this class is
 * inherently <b>not</b> thread safe (since {@code JsonNode} is mutable). It is
 * therefore the responsibility of the caller to ensure that the calling context
 * is safe (by ensuring, for instance, that only the diff operation has
 * references to the values to be diff'ed).</p>
 */
public final class JsonDiff {
    private static final JsonNodeFactory FACTORY = JacksonUtils.nodeFactory();

    private static final Equivalence<JsonNode> EQUIVALENCE
            = JsonNumEquals.getInstance();

    private JsonDiff() {
    }

    /**
     * Generate a JSON patch for transforming the source node into the target
     * node
     *
     * @param source the node to be patched
     * @param target the expected result after applying the patch
     * @return the patch as a {@link com.fasterxml.jackson.databind.JsonNode}
     */
    public static JsonNode asJson(final JsonNode source, final JsonNode target) {
        // recursively compute node diffs
        final List<Diff> diffs = new ArrayList();
        generateDiffs(diffs, JsonPointer.empty(), source, target);

        // factorize diffs to optimize patch operations
        DiffFactorizer.factorizeDiffs(diffs);

        // generate patch operations from node diffs
        final ArrayNode patch = FACTORY.arrayNode();
        for (final Diff diff : diffs) {
            patch.add(diff.asJsonPatch());
        }
        return patch;
    }

    /**
     * Generate differences between source and target node.
     *
     * @param diffs  list of differences (in order)
     * @param path   parent path for both nodes
     * @param source source node
     * @param target target node
     */
    private static void generateDiffs(final List<Diff> diffs,
                                      final JsonPointer path, final JsonNode source, final JsonNode target) {
        /*
         * If both nodes are equivalent, there is nothing to do
         */
        if (EQUIVALENCE.equivalent(source, target)) {
            return;
        }

        /*
         * Get both node types. We shortcut to a simple replace operation in the
         * following scenarios:
         *
         * - nodes are not the same type; or
         * - they are the same type, but are not containers (ie, they are
         *   neither objects nor arrays).
         */
        final NodeType sourceType = NodeType.getNodeType(source);
        final NodeType targetType = NodeType.getNodeType(target);
        if (sourceType != targetType || !source.isContainerNode()) {
            diffs.add(Diff.simpleDiff(DiffOperation.REPLACE, path, target));
            return;
        }

        /*
         * At this point, both nodes are either objects or arrays. Call the
         * appropriate diff generation methods.
         */

        if (sourceType == NodeType.OBJECT) {
            generateObjectDiffs(diffs, path, source, target);
        } else // array
        {
            generateArrayDiffs(diffs, path, source, target);
        }
    }

    /**
     * Generate differences between two object nodes
     * <p>
     * <p>Differences are generated in the following order: added members,
     * removed members, modified members.</p>
     *
     * @param diffs  list of differences (modified)
     * @param path   parent path common to both nodes
     * @param source node to patch
     * @param target node to attain
     */
    private static void generateObjectDiffs(final List<Diff> diffs,
                                            final JsonPointer path, final JsonNode source, final JsonNode target) {
        // compare different objects fieldwise in predictable order;
        // maintaining order is cosmetic, but facilitates test construction
        final List<String> inFirst = new ArrayList();
        Iterator<String> it = source.fieldNames();
        while (it.hasNext()) {
            inFirst.add(it.next());
        }
        final List<String> inSecond = new ArrayList();
        it = target.fieldNames();
        while (it.hasNext()) {
            inSecond.add(it.next());
        }

        List<String> fields;

        // added fields
        fields = new ArrayList(inSecond);
        fields.removeAll(inFirst);
        for (final String s : fields) {
            diffs.add(Diff.simpleDiff(DiffOperation.ADD, path.append(s), target.get(s)));
        }

        // removed fields
        fields = new ArrayList(inFirst);
        fields.removeAll(inSecond);
        for (final String s : fields) {
            diffs.add(Diff.simpleDiff(DiffOperation.REMOVE, path.append(s), source.get(s)));
        }

        // recursively generate diffs for fields in both objects
        fields = new ArrayList(inFirst);
        fields.retainAll(inSecond);
        for (final String s : fields) {
            generateDiffs(diffs, path.append(s), source.get(s), target.get(s));
        }
    }

    /**
     * Generate differences between two array nodes.
     * <p>
     * <p>Differences are generated in order by comparing elements against the
     * longest common subsequence of elements in both arrays.</p>
     *
     * @param diffs  list of differences (modified)
     * @param path   parent pointer of both array nodes
     * @param source array node to be patched
     * @param target target node after patching
     * @see LeastCommonSubsequence#getLCS(com.fasterxml.jackson.databind.JsonNode, com.fasterxml.jackson.databind.JsonNode)
     */
    private static void generateArrayDiffs(final List<Diff> diffs,
                                           final JsonPointer path, final JsonNode source, final JsonNode target) {
        // compare array elements linearly using longest common subsequence
        // algorithm applied to the array elements
        final IndexedJsonArray src = new IndexedJsonArray(source);
        final IndexedJsonArray dst = new IndexedJsonArray(target);
        final IndexedJsonArray lcs = LeastCommonSubsequence.doLCS(source, target);

        preLCS(diffs, path, lcs, src, dst);
        inLCS(diffs, path, lcs, src, dst);
        postLCS(diffs, path, src, dst);
    }

    /*
     * First method entered when computing array diffs. It will exit early if
     * the LCS is empty.
     *
     * If the LCS is not empty, it means that both the source and target arrays
     * have at least one element left. In such a situation, this method will run
     * until elements extracted from both arrays are equivalent to the first
     * element of the LCS.
     */
    private static void preLCS(final List<Diff> diffs, final JsonPointer path,
                               final IndexedJsonArray lcs, final IndexedJsonArray source,
                               final IndexedJsonArray target) {
        if (lcs.isEmpty()) {
            return;
        }
        /*
         * This is our sentinel: if nodes from both the first array and the
         * second array are equivalent to this node, we are done.
         */
        final JsonNode sentinel = lcs.getElement();

        /*
         * Those two variables hold nodes for the first and second array in the
         * main loop.
         */
        JsonNode srcNode;
        JsonNode dstNode;

        /*
         * This records the number of equivalences between the LCS node and
         * nodes from the source and target arrays.
         */
        int nrEquivalences;

        while (true) {
            /*
             * At each step, we reset the number of equivalences to 0.
             */
            nrEquivalences = 0;
            srcNode = source.getElement();
            dstNode = target.getElement();
            if (EQUIVALENCE.equivalent(sentinel, srcNode)) {
                nrEquivalences++;
            }
            if (EQUIVALENCE.equivalent(sentinel, dstNode)) {
                nrEquivalences++;
            }
            /*
             * If both srcNode and dstNode are equivalent to our sentinel, we
             * are done; this is our exit condition.
             */
            if (nrEquivalences == 2) {
                return;
            }
            /*
             * If none of them are equivalent to the LCS node, compute diffs
             * in first array so that the element in this array's index be
             * transformed into the matching element in the second array; then
             * restart the loop.
             *
             * Note that since we are using an LCS, and no element of either
             * array is equivalent to the first element of the LCS (our
             * sentinel), a consequence is that indices in both arrays are
             * equal. In the path below, we could have equally used the index
             * from the target array.
             */
            if (nrEquivalences == 0) {
                generateDiffs(diffs, path.append(source.getIndex()), srcNode,
                        dstNode);
                source.shift();
                target.shift();
                continue;
            }
            /*
             * If we reach this point, one array has to catch up in order to
             * reach the first element of the LCS. The logic is as follows:
             *
             * - if the source array has to catch up, it means its elements have
             *   been removed from the target array;
             * - if the target array has to catch up, it means the source
             *   array's elements are being inserted into the target array.
             */
            if (!EQUIVALENCE.equivalent(sentinel, srcNode)) {
                diffs.add(Diff.arrayRemove(path, source, target));
                source.shift();
            } else {
                diffs.add(Diff.arrayInsert(path, source, target));
                target.shift();
            }
        }
    }

    /*
     * This method is called after preLCS(). Its role is to deplete the LCS.
     *
     * One particularity of using LCS is that as long as the LCS is not empty,
     * we can be sure that there is at least one element left in both the source
     * and target array.
     */
    private static void inLCS(final List<Diff> diffs, final JsonPointer path,
                              final IndexedJsonArray lcs, final IndexedJsonArray source,
                              final IndexedJsonArray target) {
        JsonNode sourceNode;
        JsonNode targetNode;
        JsonNode lcsNode;

        boolean sourceMatch;
        boolean targetMatch;

        while (!lcs.isEmpty()) {
            sourceNode = source.getElement();
            targetNode = target.getElement();
            lcsNode = lcs.getElement();
            sourceMatch = EQUIVALENCE.equivalent(sourceNode, lcsNode);
            targetMatch = EQUIVALENCE.equivalent(targetNode, lcsNode);

            if (!sourceMatch) {
                /*
                 * At this point, the first element of our source array has
                 * failed to "reach" a matching element in the target array.
                 *
                 * Such an element therefore needs to be removed from the target
                 * array. We therefore generate a "remove event", shift the
                 * source array and restart the loop.
                 */
                diffs.add(Diff.arrayRemove(path, source, target));
                source.shift();
                continue;
            }
            /*
             * When we reach this point, we know that the element extracted
             * from the source array is equivalent to the LCS element.
             *
             * Note that from this point on, whatever the target element is, we
             * need to shift our target array; there are two different scenarios
             * we must account for:
             *
             * - if the target element is equivalent to the LCS element, we have
             *   a common subsequence element (remember that the source element
             *   is also equivalent to this same LCS element at this point); no
             *   mutation of the target array takes place; we must therefore
             *   shift all three arrays (source, target, LCS);
             * - otherwise (target element is not equivalent to the LCS
             *   element), we need to emit an insertion event of the target
             *   element, and advance the target array only.
             */
            if (targetMatch) {
                source.shift();
                lcs.shift();
            } else {
                diffs.add(Diff.arrayInsert(path, source, target));
            }
            /*
             * Shift/advance the target array; always performed, see above
             */
            target.shift();
        }
    }

    /*
     * This function is run once the LCS has been exhausted.
     *
     * Since the LCS has been exhausted, it means that for whatever nodes node1
     * and node2 extracted from source and target, they can never be equal.
     *
     * The algorithm is therefore as follows:
     *
     * - as long as both are not empty, grab both elements from both arrays and
     *   generate diff operations on them recursively;
     * - when we are out of this loop, add any elements remaining in the second
     *   array (if any), and remove any elements remaining in the first array
     *  (if any).
     *
     * Note that at the second step, only one of the two input arrays will ever
     * have any elements left; it is therefore safe to call the appropriate
     * functions for _both_ possibilities since only one will ever produce any
     * results.
     */
    private static void postLCS(final List<Diff> diffs, final JsonPointer path,
                                final IndexedJsonArray source, final IndexedJsonArray target) {
        JsonNode src, dst;

        while (!(source.isEmpty() || target.isEmpty())) {
            src = source.getElement();
            dst = target.getElement();
            generateDiffs(diffs, path.append(source.getIndex()), src, dst);
            source.shift();
            target.shift();
        }
        addRemaining(diffs, path, target);
        removeRemaining(diffs, path, source, target.size());
    }

    private static void addRemaining(final List<Diff> diffs,
                                     final JsonPointer path, final IndexedJsonArray array) {
        Diff diff;
        JsonNode node;

        while (!array.isEmpty()) {
            node = array.getElement().deepCopy();
            diff = Diff.arrayAdd(path, node);
            diffs.add(diff);
            array.shift();
        }
    }

    private static void removeRemaining(final List<Diff> diffs,
                                        final JsonPointer path, final IndexedJsonArray array,
                                        final int removeIndex) {
        Diff diff;
        JsonNode node;

        while (!array.isEmpty()) {
            node = array.getElement();
            diff = Diff.tailArrayRemove(path, array.getIndex(),
                    removeIndex, node);
            diffs.add(diff);
            array.shift();
        }
    }
}
