package io.github.reggert.cumulative.core.scan;


import io.github.reggert.cumulative.core.data.ColumnFamily;
import io.github.reggert.cumulative.core.data.ColumnIdentifier;
import io.github.reggert.cumulative.core.data.ColumnQualifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of a column or column family to include in a scan.
 */
public final class ColumnSelector implements Serializable {
    private static final long serialVersionUID = 1L;
    private final ColumnFamily family;
    private final ColumnQualifier qualifier;


    private ColumnSelector(
        @Nonnull final ColumnFamily family,
        @Nullable final ColumnQualifier qualifier
    ) {
        this.family = family;
        this.qualifier = qualifier;
    }


    /**
     * Constructs a column selector that selects an entire column family.
     *
     * @param family
     * the column family to select.
     * @return a column selector.
     */
    public static ColumnSelector entireFamily(@Nonnull final ColumnFamily family) {
        return new ColumnSelector(requireNonNull(family, "family"), null);
    }


    /**
     * Constructs a column selector that selects a specific column (family and qualifier).
     *
     * @param columnIdentifier
     * the column to select.
     * @return a column selector.
     */
    public static ColumnSelector specificColumn(@Nonnull final ColumnIdentifier columnIdentifier) {
        requireNonNull(columnIdentifier, "columnIdentifier");
        return new ColumnSelector(columnIdentifier.getFamily(), columnIdentifier.getQualifier());
    }


    /**
     * Returns the column family that this selects.
     */
    public ColumnFamily getFamily() {
        return family;
    }


    /**
     * Returns the specific column qualifier that this selects, if applicable.
     *
     * @return an optional column qualifier.
     */
    public Optional<ColumnQualifier> getQualifier() {
        return Optional.ofNullable(qualifier);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ColumnSelector that = (ColumnSelector) o;
        return Objects.equals(this.family, that.family) && Objects.equals(this.qualifier, that.qualifier);
    }


    @Override
    public int hashCode() {
        return Objects.hash(family, qualifier);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ColumnSelector{");
        sb.append("family=").append(family);
        if (qualifier != null) {
            sb.append(", qualifier=").append(qualifier);
        }
        sb.append('}');
        return sb.toString();
    }
}
