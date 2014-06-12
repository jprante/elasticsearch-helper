package org.xbib.json.pointer;

import com.fasterxml.jackson.core.TreeNode;

/**
 * Reference token traversal class
 * <p>
 * <p>This class is meant to be extended and implemented for all types of trees
 * inheriting {@link com.fasterxml.jackson.core.TreeNode}.</p>
 * <p>
 * <p>This package contains one implementation of this class for {@link
 * com.fasterxml.jackson.databind.JsonNode}.</p>
 * <p>
 * <p>Note that its {@link #equals(Object)}, {@link #hashCode()} and {@link
 * #toString()} are final.</p>
 *
 * @param <T> the type of tree to traverse
 * @see JsonNodeResolver
 */
public abstract class TokenResolver<T extends TreeNode> {
    /**
     * The associated reference token
     */
    protected final ReferenceToken token;

    /**
     * The only constructor
     *
     * @param token the reference token
     */
    protected TokenResolver(final ReferenceToken token) {
        this.token = token;
    }

    /**
     * Advance one level into the tree
     * <p>
     * <p>Note: it is <b>required</b> that this method return null on
     * traversal failure.</p>
     * <p>
     * <p>Note 2: handling {@code null} itself is up to implementations.</p>
     *
     * @param node the node to traverse
     * @return the other node, or {@code null} if no such node exists for that
     * token
     */
    public abstract T get(final T node);

    public final ReferenceToken getToken() {
        return token;
    }

    @Override
    public final int hashCode() {
        return token.hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TokenResolver<?> other = (TokenResolver<?>) obj;
        return token.equals(other.token);
    }

    @Override
    public final String toString() {
        return token.toString();
    }
}
