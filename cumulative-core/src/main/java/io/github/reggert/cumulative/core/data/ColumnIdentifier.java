package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable key of a column of an Accumulo table, consisting of a family and qualifier.
 */
public final class ColumnIdentifier implements Serializable, Comparable<ColumnIdentifier> {
    private static final long serialVersionUID = 1L;
    /**
     * Empty column identifier.
     *
     * Mainly used for tables that are simple key-value mappings.
     */
    public static final ColumnIdentifier EMPTY =
        new ColumnIdentifier(ColumnFamily.EMPTY, ColumnQualifier.EMPTY);
    private final ColumnFamily family;
    private final ColumnQualifier qualifier;
    private static final Comparator<ColumnIdentifier> COMPARATOR =
        Comparator.comparing(ColumnIdentifier::getFamily).thenComparing(ColumnIdentifier::getQualifier);


    /**
     * Constructs a {@code ColumnIdentifier} from the specified family and qualifier.
     *
     * @param family
     * the major part of the column key.
     * @param qualifier
     * the minor part of the column key.
     */
    public ColumnIdentifier(@Nonnull final ColumnFamily family, @Nonnull final ColumnQualifier qualifier) {
        this.family = requireNonNull(family, "family");
        this.qualifier = requireNonNull(qualifier, "qualifier");
    }


    /**
     * Convenience method to construct a simple {@code ColumnIdentifier} consisting of only a string family
     * with no qualifier.
     *
     * @param family
     * the column family as a {@code String}.
     * @return a new {@code ColumnIdentifier} with empty qualifier.
     */
    public static ColumnIdentifier fromString(@Nonnull final String family) {
        return new ColumnIdentifier(ColumnFamily.fromString(family), ColumnQualifier.EMPTY);
    }


    /**
     * Constructs a {@code ColumnIdentifier} by extracting the family and qualifier from an Accumulo
     * {@code Key} object.
     *
     * @param accumuloKey
     * the {@code Key} object from which to extract values.
     * @return a new {@code ColumnIdentifier}.
     */
    public static ColumnIdentifier fromAccumuloKey(@Nonnull final Key accumuloKey) {
        return new ColumnIdentifier(
            ColumnFamily.fromAccumuloKey(accumuloKey),
            ColumnQualifier.fromAccumuloKey(accumuloKey)
        );
    }


    /**
     * Returns the major part of the column key.
     */
    public ColumnFamily getFamily() {
        return family;
    }


    /**
     * Returns the minor part of the column key.
     */
    public ColumnQualifier getQualifier() {
        return qualifier;
    }


    @Override
    public int compareTo(@Nonnull final ColumnIdentifier that) {
        return COMPARATOR.compare(this, that);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ColumnIdentifier that = (ColumnIdentifier) o;
        return Objects.equals(this.family, that.family) && Objects.equals(this.qualifier, that.qualifier);
    }


    @Override
    public int hashCode() {
        return Objects.hash(family, qualifier);
    }


    @Override
    public String toString() {
        return String.format("ColumnIdentifier{family=%s, qualifier=%s}", family, qualifier);
    }
}
