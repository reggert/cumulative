package io.github.reggert.cumulative.core.data;


import org.apache.accumulo.core.data.Key;

import javax.annotation.Nonnull;
import java.io.Serializable;
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
    private final ColumnFamily columnFamily;
    private final ColumnQualifier columnQualifier;
    private final EntryVisibility visibility;
    private final Timestamp timestamp;
    private static final Comparator<EntryKey> COMPARATOR =
        comparing(EntryKey::getRowIdentifier)
        .thenComparing(EntryKey::getColumnFamily)
        .thenComparing(EntryKey::getColumnQualifier)
        .thenComparing(EntryKey::getVisibility)
        .thenComparing(EntryKey::getTimestamp);


    /**
     * Constructs a new {@code EntryKey}.
     *
     * @param rowIdentifier
     * the rowid.
     * @param columnFamily
     * the column family.
     * @param columnQualifier
     * the column qualifier.
     * @param visibility
     * the visibility to apply to the entry.
     * @param timestamp
     * the Accumulo timestamp of the entry.
     */
    public EntryKey(
        @Nonnull final RowIdentifier rowIdentifier,
        @Nonnull final ColumnFamily columnFamily,
        @Nonnull final ColumnQualifier columnQualifier,
        @Nonnull final EntryVisibility visibility,
        @Nonnull final Timestamp timestamp
    ) {
        this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
        this.columnFamily = requireNonNull(columnFamily, "columnFamily");
        this.columnQualifier = requireNonNull(columnQualifier, "columnQualifier");
        this.visibility = requireNonNull(visibility, "visibility");
        this.timestamp = requireNonNull(timestamp, "timestamp");
    }


    /**
     * Constructs a new {@code EntryKey}, leaving the timestamp unspecified.
     *
     * @param rowIdentifier
     * the rowid.
     * @param columnFamily
     * the column family.
     * @param columnQualifier
     * the column qualifier.
     * @param visibility
     * the visibility to apply to the entry.
     */
    public EntryKey(
        @Nonnull final RowIdentifier rowIdentifier,
        @Nonnull final ColumnFamily columnFamily,
        @Nonnull final ColumnQualifier columnQualifier,
        @Nonnull final EntryVisibility visibility
    ) {
        this(rowIdentifier, columnFamily, columnQualifier, visibility, Timestamp.UNSPECIFIED);
    }


    /**
     * Constructs a new {@code EntryKey}, using the default visibility and leaving the timestamp unspecified.
     *
     * @param rowIdentifier
     * the rowid.
     * @param columnFamily
     * the column family.
     * @param columnQualifier
     * the column qualifier.
     */
    public EntryKey(
        @Nonnull final RowIdentifier rowIdentifier,
        @Nonnull final ColumnFamily columnFamily,
        @Nonnull final ColumnQualifier columnQualifier
    ) {
        this(rowIdentifier, columnFamily, columnQualifier, EntryVisibility.DEFAULT);
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
            ColumnFamily.fromAccumuloKey(accumuloKey),
            ColumnQualifier.fromAccumuloKey(accumuloKey),
            EntryVisibility.fromAccumuloKey(accumuloKey),
            new Timestamp(accumuloKey.getTimestamp())
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
            columnFamily.toByteArray(),
            columnQualifier.toByteArray(),
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
     * Returns the column family.
     */
    public ColumnFamily getColumnFamily() {
        return columnFamily;
    }


    /**
     * Returns the column qualifier.
     */
    public ColumnQualifier getColumnQualifier() {
        return columnQualifier;
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
            Objects.equals(this.columnFamily, that.columnFamily) &&
            Objects.equals(this.columnQualifier, that.columnQualifier) &&
            Objects.equals(this.visibility, that.visibility) &&
            Objects.equals(this.timestamp, that.timestamp);
    }


    @Override
    public int hashCode() {
        return Objects.hash(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp);
    }


    @Override
    public String toString() {
        return String.format(
            "EntryKey{rowIdentifier=%s, columnFamily=%s, columnQualifier=%s, visibility=%s, timestamp=%s}",
            rowIdentifier,
            columnFamily,
            columnQualifier,
            visibility,
            timestamp
        );
    }
}
