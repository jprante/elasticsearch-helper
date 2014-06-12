/**
 * JSON diff implementation
 *
 * <p>The main class, {@link org.xbib.json.diff.JsonDiff}, does the
 * reverse of what JSON Patch does: given two JSON values, it generates a patch
 * (as JSON) to apply to the first node in order to obtain the second node.</p>
 *
 * <p>This implementation is able to factorize additions and removals into
 * moves and copies.</p>
 */
package org.xbib.json.diff;
