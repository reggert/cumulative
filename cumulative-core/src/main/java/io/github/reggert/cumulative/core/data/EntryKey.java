package io.github.reggert.cumulative.core.data;


import org.apache.accumulo.core.data.Key;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.sql.Time;
import java.util.Comparator;
import java.util.Objects;

import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo {@code Key}.
 *
 * Note that this class omits the delete flag. If you need to mess with something that low-level, use the
 * raw Accumulo API.
 */
public final class EntryKey implements Serializable, Comparable<EntryKey> {
    private static final long serialVersionUID = 1L;
    private final RowIdentifier rowIdentifier;
    private final ColumnIdentifier columnIdentifier;
    private final EntryVisibility visibility;
    private final Timestamp timestamp;
    private static final Comparator<EntryKey> COMPARATOR =
        comparing(EntryKey::getRowIdentifier)
        .thenComparing(EntryKey::getColumnIdentifier)
        .thenComparing(EntryKey::getVisibility)
        .thenComparing(EntryKey::getTimestamp);


    /**
     * Constructs a new {@code EntryKey}.
     *
     * @param rowIdentifier
     * the rowid.
     * @param columnIdentifier
     * the column key.
     * @param visibility
     * the visibility to apply to the entry.
     * @param timestamp
     * the Accumulo timestamp of the entry.
     */
    public EntryKey(
        @Nonnull final RowIdentifier rowIdentifier,
        @Nonnull final ColumnIdentifier columnIdentifier,
        @Nonnull final EntryVisibility visibility,
        @Nonnull final Timestamp timestamp
    ) {
        this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
        this.columnIdentifier = requireNonNull(columnIdentifier, "columnIdentifier");
        this.visibility = requireNonNull(visibility, "visibility");
        this.timestamp = requireNonNull(timestamp, "timestamp");
    }


    /**
     * Constructs a new {@code EntryKey}, leaving the timestamp unspecified.
     *
     * @param rowIdentifier
     * the rowid.
     * @param columnIdentifier
     * the column key.
     * @param visibility
     * the visibility to apply to the entry.
     */
    public EntryKey(
        @Nonnull final RowIdentifier rowIdentifier,
        @Nonnull final ColumnIdentifier columnIdentifier,
        @Nonnull final EntryVisibility visibility
    ) {
        this(rowIdentifier, columnIdentifier, visibility, Timestamp.UNSPECIFIED);
    }


    /**
     * Constructs a new {@code EntryKey}, using the default visibility and leaving the timestamp unspecified.
     *
     * @param rowIdentifier
     * the rowid.
     * @param columnIdentifier
     * the column key.
     */
    public EntryKey(
        @Nonnull final RowIdentifier rowIdentifier,
        @Nonnull final ColumnIdentifier columnIdentifier
    ) {
        this(rowIdentifier, columnIdentifier, EntryVisibility.DEFAULT);
    }


    /**
     * Constructs an {@code EntryKey} from an Accumulo {@code Key} object.
     *
     * @param accumuloKey
     * the {@code Key} from which to extract values.
     * @return a new {@code EntryKey}.
     */
    public static EntryKey fromAccumuloKey(@Nonnull final Key accumuloKey) {
        requireNonNull(accumuloKey, "accumuloKey");
        return new EntryKey(
            RowIdentifier.fromAccumuloKey(accumuloKey),
            ColumnIdentifier.fromAccumuloKey(accumuloKey),
            EntryVisibility.fromAccumuloKey(accumuloKey),
            Timestamp.fromAccumuloKey(accumuloKey)
        );
    }


    /**
     * Constructs an Accumulo {@code Key} from this object.
     *
     * @return a new {@code Key}.
     */
    public Key toAccumuloKey() {
        return new Key(
            rowIdentifier.toByteArray(),
            columnIdentifier.getFamily().toByteArray(),
            columnIdentifier.getQualifier().toByteArray(),
            visibility.toByteArray(),
            timestamp.longValue(),
            false,
            false
        );
    }



    /**
     * Returns the rowid.
     */
    public RowIdentifier getRowIdentifier() {
        return rowIdentifier;
    }


    /**
     * Returns the column key.
     */
    public ColumnIdentifier getColumnIdentifier() {
        return columnIdentifier;
    }


    /**
     * Returns the entry visibility.
     */
    public EntryVisibility getVisibility() {
        return visibility;
    }


    /**
     * Returns the entry timestamp.
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }


    @Override
    public int compareTo(@Nonnull final EntryKey that) {
        return COMPARATOR.compare(this, that);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final EntryKey that = (EntryKey) o;
        return Objects.equals(this.rowIdentifier, that.rowIdentifier) &&
            Objects.equals(this.columnIdentifier, that.columnIdentifier) &&
            Objects.equals(this.visibility, that.visibility) &&
            Objects.equals(this.timestamp, that.timestamp);
    }


    @Override
    public int hashCode() {
        return Objects.hash(rowIdentifier, columnIdentifier, visibility, timestamp);
    }


    @Override
    public String toString() {
        return String.format(
            "EntryKey{rowIdentifier=%s, columnIdentifier=%s, visibility=%s, timestamp=%s}",
            rowIdentifier,
            columnIdentifier,
            visibility,
            timestamp
        );
    }
}
