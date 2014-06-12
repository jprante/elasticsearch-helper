package org.xbib.json.jackson;

import java.io.Serializable;

public final class Wrapper<T> implements Serializable {

    private final Equivalence<? super T> equivalence;

    private final T reference;

    public Wrapper(Equivalence<? super T> equivalence, T reference) {
        this.equivalence = equivalence;
        this.reference = reference;
    }

    /**
     * Returns the (possibly null) reference wrapped by this instance.
     */
    public T get() {
        return reference;
    }

    /**
     * Returns {@code true} if {@link Equivalence#equivalent(Object, Object)} applied to the wrapped
     * references is {@code true} and both wrappers use the {@link Object#equals(Object) same}
     * equivalence.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Wrapper) {
            Wrapper<?> that = (Wrapper<?>) obj;
        /*
         * We cast to Equivalence<Object> here because we can't check the type of the reference held
         * by the other wrapper.  But, by checking that the Equivalences are equal, we know that
         * whatever type it is, it is assignable to the type handled by this wrapper's equivalence.
         */
            @SuppressWarnings("unchecked")
            Equivalence<Object> equivalence = (Equivalence<Object>) this.equivalence;
            return equivalence.equals(that.equivalence)
                    && equivalence.equivalent(this.reference, that.reference);
        } else {
            return false;
        }
    }

    /**
     * Returns the result of {@link Equivalence#hash(Object)} applied to the the wrapped reference.
     */
    @Override
    public int hashCode() {
        return equivalence.hash(reference);
    }

    /**
     * Returns a string representation for this equivalence wrapper. The form of this string
     * representation is not specified.
     */
    @Override
    public String toString() {
        return equivalence + ".wrap(" + reference + ")";
    }

    private static final long serialVersionUID = 0;
}
