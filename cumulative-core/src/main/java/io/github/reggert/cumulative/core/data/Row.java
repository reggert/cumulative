package io.github.reggert.cumulative.core.data;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of a group of (usually all) the entries in an Accumulo row.
 *
 * Note that although Accumulo will support multiple entries in the same column with different
 * visibilities, this class assumes a single entry per column, which is generally how people expect tables
 * to operate.
 */
public final class Row implements Serializable {
    private final RowIdentifier identifier;
    private final SortedMap<ColumnIdentifier, Entry> entries;


    private Row(@Nonnull final RowIdentifier identifier, @Nonnull final SortedMap<ColumnIdentifier, Entry> entries) {
        this.identifier = identifier;
        this.entries = entries;
    }


    /**
     * Builder for {@code Row} objects that acts as a {@code Consumer} of entries.
     *
     * If two entries have the same column identifier, the one with the latter timestamp wins.
     * If the timestamps are the same, the one with the lesser-sorting visibility wins.
     * Otherwise, the first entry consumed wins.
     */
    public static final class Builder implements Consumer<Entry> {
        private RowIdentifier identifier;
        private TreeMap<ColumnIdentifier, Entry> entries;

        private Builder() {}

        private Builder(@Nonnull final Row row) {
            this.identifier = row.getIdentifier();
            this.entries = new TreeMap<>(row.entries);
        }

        @Override
        public void accept(@Nonnull final Entry entry) {
            requireNonNull(entry, "entry");
            if (this.entries == null) {
                entries = new TreeMap<>();
            }
            if (canAccept(entry)) {
                this.identifier = entry.getKey().getRowIdentifier();
                final Entry existingEntry = entries.get(entry.getKey().getColumnIdentifier());
                if (existingEntry == null || canReplace(existingEntry, entry)) {
                    entries.put(entry.getKey().getColumnIdentifier(), entry);
                }
            }
            else {
                throw new IllegalArgumentException(
                    String.format(
                        "Mismatched row (%s != %s)",
                        this.identifier,
                        entry.getKey().getRowIdentifier()
                    )
                );
            }
        }

        private static boolean canReplace(@Nonnull final Entry existingEntry, @Nonnull final Entry replacement) {
            final int tsCmp =
                existingEntry.getKey().getTimestamp().compareTo(replacement.getKey().getTimestamp());
            if (tsCmp == 0) {
                // If timestamps are the same, lesser visibility wins.
                final int visCmp =
                    existingEntry.getKey().getVisibility().compareTo(replacement.getKey().getVisibility());
                return visCmp > 0;
            }
            else {
                return tsCmp < 0;
            }
        }

        /**
         * Indicates whether this builder can currently accept the specified entry.
         *
         * In order to be acceptable, either the builder must be empty, or the entry must have the same row
         * identifier as is currently stored in the builder. Calling {@link #accept(Entry)} on an unacceptable
         * {@code Entry} will result in an {@code IllegalArgumentException}.
         *
         * @param entry
         * the entry to check.
         * @return {@code true}
         */
        public boolean canAccept(@Nonnull final Entry entry) {
            return isEmpty() || this.identifier.equals(entry.getKey().getRowIdentifier());
        }

        /**
         * Builds a {@code Row} and resets the builder to a pristine state.
         *
         * @return a new {@code Row object}.
         * @throws IllegalStateException if no entries have been consumed by the builder.
         */
        public Row build() {
            if (this.identifier == null) {
                throw new IllegalStateException("row contains no entries");
            }
            final Row row = new Row(identifier, Collections.unmodifiableSortedMap(entries));
            reset();
            return row;
        }


        /**
         * Resets the builder to a pristine (empty) state.
         */
        public void reset() {
            this.identifier = null;
            this.entries = null;
        }


        /**
         * Indicates whether this builder is empty.
         *
         * @return {@code true} if the builder is empty, {@code false} if it contains any entries.
         */
        public boolean isEmpty() {
            return this.identifier == null;
        }
    }


    private static final class RowIterator implements Iterator<Row> {
        private final Iterator<Entry> entryIterator;
        private final Builder builder = builder();

        private RowIterator(@Nonnull final Iterator<Entry> entryIterator) {
            this.entryIterator = requireNonNull(entryIterator, "entryIterator");
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext() || !builder.isEmpty();
        }

        @Override
        public Row next() {
            if (!entryIterator.hasNext() && builder.isEmpty()) {
                throw new NoSuchElementException("No entries to process");
            }
            while (entryIterator.hasNext()) {
                final Entry nextEntry = entryIterator.next();
                if (builder.canAccept(nextEntry)) {
                    builder.accept(nextEntry);
                }
                else {
                    final Row row = builder.build();
                    builder.accept(nextEntry);
                    return row;
                }
            }
            return builder.build();
        }
    }


    /**
     * Create a builder to construct a new {@code Row}.
     *
     * @return a new {@code Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }


    /**
     * Constructs a {@code Row} from a collection of entries.
     *
     * @param entries
     * a collection of entries for the same row. Must have the same row identifier and be non-empty.
     * @return a new {@code Row}.
     * @throws IllegalArgumentException if the row identifiers are not consistent.
     * @throws IllegalStateException if the collection is empty.
     */
    public static Row fromEntries(@Nonnull final Iterable<Entry> entries) {
        final Builder builder = builder();
        entries.forEach(builder);
        return builder.build();
    }


    /**
     * Converts an iterator of entries into an iterator of rows.
     *
     * Note that this assumes that all entries for a given row will appear adjacent to each other in the source
     * iterator, which is how they will be returned from Accumulo scans. Otherwise, multiple {@code Row}
     * objects may be created for the same {@code RowIdentifier}.
     *
     * @param entryIterator
     * iterator of entries to consume.
     * @return an iterator that returns {@code Row} objects.
     */
    public static Iterator<Row> iterator(@Nonnull final Iterator<Entry> entryIterator) {
        return new RowIterator(entryIterator);
    }


    /**
     * Returns the {@link RowIdentifier} of the row.
     */
    public RowIdentifier getIdentifier() {
        return identifier;
    }


    /**
     * Returns an immutable map of the entries in the row, keyed by column.
     *
     * @return an immutable map.
     */
    public SortedMap<ColumnIdentifier, Entry> getEntries() {
        return entries;
    }


    /**
     * Creates a new builder pre-populated with the entries from this row.
     *
     * This can be useful for creating a modified version of the row. Note that the same rules regarding
     * duplicate columns apply as when building from scratch.
     *
     * @return a new {@code Builder} containing all the entries from this row.
     */
    public Builder toBuilder() {
        return new Builder(this);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Row that = (Row) o;
        return Objects.equals(this.identifier, that.identifier) && Objects.equals(this.entries, that.entries);
    }


    @Override
    public int hashCode() {
        return Objects.hash(identifier, entries);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Row{");
        sb.append("identifier=").append(identifier);
        sb.append(", entries=").append(entries);
        sb.append('}');
        return sb.toString();
    }
}
