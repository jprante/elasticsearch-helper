/**
 * JSON Pointer related classes
 *
 * <p>This package, while primarily centered on {@link
 * org.xbib.json.pointer.JsonPointer}, is a generalization of JSON
 * Pointer to all implementations of Jackson's {@link
 * com.fasterxml.jackson.core.TreeNode}.</p>
 *
 * <p>The fundamentals of JSON Pointer remain the same, however: a JSON pointer
 * is a set of reference tokens separated by the {@code /} character. One
 * reference token is materialized by the {@link
 * org.xbib.json.pointer.ReferenceToken} class, and advancing
 * one level into a tree is materialized by {@link
 * org.xbib.json.pointer.TokenResolver}. A {@link
 * org.xbib.json.pointer.TreePointer} is a collection of token
 * resolvers.</p>
 */
package org.xbib.json.pointer;
