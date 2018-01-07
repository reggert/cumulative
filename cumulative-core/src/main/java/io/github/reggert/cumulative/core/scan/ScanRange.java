package io.github.reggert.cumulative.core.scan;

import io.github.reggert.cumulative.core.data.*;
import org.apache.accumulo.core.data.Range;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


public abstract class ScanRange implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final FullTable FULL_TABLE = new FullTable();

    private ScanRange() {}


    /**
     * Returns the equivalent Accumulo {@code Range} object.
     */
    public abstract Range toAccumuloRange();


    /**
     * Determines whether this range contains the specified key.
     *
     * @param key
     * the key to check.
     * @return {@code true} if the key is within the range, {@code false} otherwise.
     */
    public final boolean contains(@Nonnull EntryKey key) {
        return toAccumuloRange().contains(key.toAccumuloKey());
    }


    /**
     * Range for a full table scan.
     */
    public static final class FullTable extends ScanRange {
        private static final long serialVersionUID = 1L;

        private FullTable() {}

        @Override
        public Range toAccumuloRange() {
            return new Range();
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof FullTable;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass());
        }

        @Override
        public String toString() {
            return "FullTable";
        }
    }


    /**
     * Range matching an entire row.
     */
    public static final class ExactRow extends ScanRange implements Comparable<ExactRow> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;

        /**
         * Constructs a range matching the specified row identifier.
         *
         * @param rowIdentifier
         * the row identifier to match.
         */
        public ExactRow(@Nonnull final RowIdentifier rowIdentifier) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
        }

        /**
         * Returns the row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        @Override
        public Range toAccumuloRange() {
            return Range.exact(rowIdentifier.toHadoopText());
        }

        @Override
        public int compareTo(@Nonnull final ExactRow that) {
            return this.rowIdentifier.compareTo(that.rowIdentifier);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ExactRow that = (ExactRow) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ExactRow{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching an entire column family within a row.
     */
    public static final class ExactColumnFamily extends ScanRange implements Comparable<ExactColumnFamily> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;
        private final ColumnFamily columnFamily;
        private static final Comparator<ExactColumnFamily> COMPARATOR =
            Comparator.comparing(ExactColumnFamily::getRowIdentifier)
                .thenComparing(ExactColumnFamily::getColumnFamily);

        /**
         * Constructs a range matching the specified column family within the specified row.
         *
         * @param rowIdentifier
         * the row identifier to match.
         * @param columnFamily
         * the column family to match.
         */
        public ExactColumnFamily(
            @Nonnull final RowIdentifier rowIdentifier,
            @Nonnull final ColumnFamily columnFamily
        ) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
            this.columnFamily = requireNonNull(columnFamily, "columnFamily");
        }

        /**
         * Returns the row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        /**
         * Returns the column family that this range matches.
         */
        public ColumnFamily getColumnFamily() {
            return columnFamily;
        }

        @Override
        public Range toAccumuloRange() {
            return Range.exact(rowIdentifier.toHadoopText(), columnFamily.toHadoopText());
        }

        @Override
        public int compareTo(@Nonnull final ExactColumnFamily that) {
            return COMPARATOR.compare(this, that);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ExactColumnFamily that = (ExactColumnFamily) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier) &&
                Objects.equals(this.columnFamily, that.columnFamily);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier, columnFamily);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ExactColumnFamily{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append(", columnFamily=").append(columnFamily);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching an particular column within a row.
     */
    public static final class ExactColumn extends ScanRange implements Comparable<ExactColumn> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;
        private final ColumnIdentifier columnIdentifier;
        private static final Comparator<ExactColumn> COMPARATOR =
            Comparator.comparing(ExactColumn::getRowIdentifier)
                .thenComparing(ExactColumn::getColumnIdentifier);

        /**
         * Constructs a range matching the specified column within the specified row.
         *
         * @param rowIdentifier
         * the row identifier to match.
         * @param columnIdentifier
         * the column to match.
         */
        public ExactColumn(
            @Nonnull final RowIdentifier rowIdentifier,
            @Nonnull final ColumnIdentifier columnIdentifier
        ) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
            this.columnIdentifier = requireNonNull(columnIdentifier, "columnIdentifier");
        }

        /**
         * Returns the row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        /**
         * Returns the column that this range matches.
         */
        public ColumnIdentifier getColumnIdentifier() {
            return columnIdentifier;
        }

        @Override
        public Range toAccumuloRange() {
            return Range.exact(
                rowIdentifier.toHadoopText(),
                columnIdentifier.getFamily().toHadoopText(),
                columnIdentifier.getQualifier().toHadoopText()
            );
        }

        @Override
        public int compareTo(@Nonnull final ExactColumn that) {
            return COMPARATOR.compare(this, that);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ExactColumn that = (ExactColumn) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier) &&
                Objects.equals(this.columnIdentifier, that.columnIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier, columnIdentifier);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ExactColumn{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append(", columnIdentifier=").append(columnIdentifier);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching an particular column with a particular visibility within a row.
     */
    public static final class ExactVisibility extends ScanRange implements Comparable<ExactVisibility> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;
        private final ColumnIdentifier columnIdentifier;
        private final EntryVisibility visibility;
        private static final Comparator<ExactVisibility> COMPARATOR =
            Comparator.comparing(ExactVisibility::getRowIdentifier)
                .thenComparing(ExactVisibility::getColumnIdentifier)
                .thenComparing(ExactVisibility::getVisibility);

        /**
         * Constructs a range matching the specified column and visibility within the specified row.
         *
         * @param rowIdentifier
         * the row identifier to match.
         * @param columnIdentifier
         * the column to match.
         * @param visibility
         * the visibility to match.
         */
        public ExactVisibility(
            @Nonnull final RowIdentifier rowIdentifier,
            @Nonnull final ColumnIdentifier columnIdentifier,
            @Nonnull final EntryVisibility visibility
        ) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
            this.columnIdentifier = requireNonNull(columnIdentifier, "columnIdentifier");
            this.visibility = requireNonNull(visibility, "visibility");
        }

        /**
         * Returns the row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        /**
         * Returns the column that this range matches.
         */
        public ColumnIdentifier getColumnIdentifier() {
            return columnIdentifier;
        }

        /**
         * Returns the visibility that this range matches.
         */
        public EntryVisibility getVisibility() {
            return visibility;
        }

        @Override
        public Range toAccumuloRange() {
            return Range.exact(
                rowIdentifier.toHadoopText(),
                columnIdentifier.getFamily().toHadoopText(),
                columnIdentifier.getQualifier().toHadoopText(),
                visibility.toHadoopText()
            );
        }

        @Override
        public int compareTo(@Nonnull final ExactVisibility that) {
            return COMPARATOR.compare(this, that);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ExactVisibility that = (ExactVisibility) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier) &&
                Objects.equals(this.columnIdentifier, that.columnIdentifier) &&
                Objects.equals(this.visibility, that.visibility);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier, columnIdentifier, visibility);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ExactVisibility{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append(", columnIdentifier=").append(columnIdentifier);
            sb.append(", visibility=").append(visibility);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching all row identifiers with a given prefix.
     */
    public static final class RowPrefix extends ScanRange implements Comparable<RowPrefix> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowPrefix;

        /**
         * Constructs a range matching the specified row identifier.
         *
         * @param rowPrefix
         * the row identifier prefix to match.
         */
        public RowPrefix(@Nonnull final RowIdentifier rowPrefix) {
            this.rowPrefix = requireNonNull(rowPrefix, "rowPrefix");
        }

        /**
         * Returns the row identifier prefix that this range matches.
         */
        public RowIdentifier getRowPrefix() {
            return rowPrefix;
        }

        @Override
        public Range toAccumuloRange() {
            return Range.prefix(rowPrefix.toHadoopText());
        }

        @Override
        public int compareTo(@Nonnull final RowPrefix that) {
            return this.rowPrefix.compareTo(that.rowPrefix);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RowPrefix that = (RowPrefix) o;
            return Objects.equals(this.rowPrefix, that.rowPrefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowPrefix);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RowPrefix{");
            sb.append("rowPrefix=").append(rowPrefix);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching all column families starting within a given prefix within a row.
     */
    public static final class ColumnFamilyPrefix extends ScanRange implements Comparable<ColumnFamilyPrefix> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;
        private final ColumnFamily columnFamilyPrefiix;
        private static final Comparator<ColumnFamilyPrefix> COMPARATOR =
            Comparator.comparing(ColumnFamilyPrefix::getRowIdentifier)
                .thenComparing(ColumnFamilyPrefix::getColumnFamilyPrefix);

        /**
         * Constructs a range matching the specified column family within the specified row.
         *
         * @param rowIdentifier
         * the row identifier to match.
         * @param columnFamilyPrefiix
         * the column family prefix to match.
         */
        public ColumnFamilyPrefix(
            @Nonnull final RowIdentifier rowIdentifier,
            @Nonnull final ColumnFamily columnFamilyPrefiix
        ) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
            this.columnFamilyPrefiix = requireNonNull(columnFamilyPrefiix, "columnFamilyPrefiix");
        }

        /**
         * Returns the row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        /**
         * Returns the column family prefix that this range matches.
         */
        public ColumnFamily getColumnFamilyPrefix() {
            return columnFamilyPrefiix;
        }

        @Override
        public Range toAccumuloRange() {
            return Range.prefix(rowIdentifier.toHadoopText(), columnFamilyPrefiix.toHadoopText());
        }

        @Override
        public int compareTo(@Nonnull final ColumnFamilyPrefix that) {
            return COMPARATOR.compare(this, that);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ColumnFamilyPrefix that = (ColumnFamilyPrefix) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier) &&
                Objects.equals(this.columnFamilyPrefiix, that.columnFamilyPrefiix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier, columnFamilyPrefiix);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ColumnFamilyPrefix{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append(", columnFamilyPrefiix=").append(columnFamilyPrefiix);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching an entire column family within a row.
     */
    public static final class ColumnQualifierPrefix extends ScanRange implements Comparable<ColumnQualifierPrefix> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;
        private final ColumnFamily columnFamily;
        private final ColumnQualifier columnQualifierPrefix;
        private static final Comparator<ColumnQualifierPrefix> COMPARATOR =
            Comparator.comparing(ColumnQualifierPrefix::getRowIdentifier)
                .thenComparing(ColumnQualifierPrefix::getColumnFamily)
                .thenComparing(ColumnQualifierPrefix::getColumnQualifierPrefix);

        /**
         * Constructs a range matching the specified column family within the specified row.
         *
         * @param rowIdentifier
         * the row identifier to match.
         * @param columnFamily
         * the column family to match.
         * @param columnQualifierPrefix
         * the column qualifier prefix to match.
         */
        public ColumnQualifierPrefix(
            @Nonnull final RowIdentifier rowIdentifier,
            @Nonnull final ColumnFamily columnFamily,
            @Nonnull final ColumnQualifier columnQualifierPrefix
        ) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
            this.columnFamily = requireNonNull(columnFamily, "columnFamily");
            this.columnQualifierPrefix = requireNonNull(columnQualifierPrefix, "columnQualifierPrefix");
        }

        /**
         * Returns the row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        /**
         * Returns the column family that this range matches.
         */
        public ColumnFamily getColumnFamily() {
            return columnFamily;
        }

        /**
         * Returns the column qualifier prefix that this range matches.
         */
        public ColumnQualifier getColumnQualifierPrefix() {
            return columnQualifierPrefix;
        }

        @Override
        public Range toAccumuloRange() {
            return Range.prefix(
                rowIdentifier.toHadoopText(),
                columnFamily.toHadoopText(),
                columnQualifierPrefix.toHadoopText()
            );
        }

        @Override
        public int compareTo(@Nonnull final ColumnQualifierPrefix that) {
            return COMPARATOR.compare(this, that);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ColumnQualifierPrefix that = (ColumnQualifierPrefix) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier) &&
                Objects.equals(this.columnFamily, that.columnFamily) &&
                Objects.equals(this.columnQualifierPrefix, that.columnQualifierPrefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier, columnFamily, columnQualifierPrefix);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ColumnQualifierPrefix{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append(", columnFamily=").append(columnFamily);
            sb.append(", columnQualifierPrefix=").append(columnQualifierPrefix);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching all rows greater than or equal to a specified minimum.
     */
    public static final class MinimumRow extends ScanRange implements Comparable<MinimumRow> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;

        /**
         * Constructs a range matching rows starting with the specified row identifier.
         *
         * @param rowIdentifier
         * the minimum row identifier to match.
         */
        public MinimumRow(@Nonnull final RowIdentifier rowIdentifier) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
        }

        /**
         * Returns the minimum row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        @Override
        public Range toAccumuloRange() {
            return new Range(rowIdentifier.toHadoopText(), null);
        }

        @Override
        public int compareTo(@Nonnull final MinimumRow that) {
            return this.rowIdentifier.compareTo(that.rowIdentifier);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final MinimumRow that = (MinimumRow) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("MinimumRow{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching all rows less than or equal to a specified maximum.
     */
    public static final class MaximumRow extends ScanRange implements Comparable<MaximumRow> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;

        /**
         * Constructs a range matching rows up to and including the specified row identifier.
         *
         * @param rowIdentifier
         * the maximum row identifier to match.
         */
        public MaximumRow(@Nonnull final RowIdentifier rowIdentifier) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
        }

        /**
         * Returns the maximum row identifier that this range matches.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        @Override
        public Range toAccumuloRange() {
            return new Range(null, rowIdentifier.toHadoopText());
        }

        @Override
        public int compareTo(@Nonnull final MaximumRow that) {
            return this.rowIdentifier.compareTo(that.rowIdentifier);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final MaximumRow that = (MaximumRow) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("MaximumRow{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching all rows greater than a specified minimum.
     */
    public static final class RowsAfter extends ScanRange implements Comparable<RowsAfter> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;

        /**
         * Constructs a range matching rows greater than the specified row identifier.
         *
         * @param rowIdentifier
         * the minimum row identifier to match.
         */
        public RowsAfter(@Nonnull final RowIdentifier rowIdentifier) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
        }

        /**
         * Returns the maximum row identifier that this range does not match.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        @Override
        public Range toAccumuloRange() {
            return new Range(rowIdentifier.toHadoopText(), false, null, true);
        }

        @Override
        public int compareTo(@Nonnull final RowsAfter that) {
            return this.rowIdentifier.compareTo(that.rowIdentifier);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RowsAfter that = (RowsAfter) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RowsAfter{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching all rows less than a specified maximum.
     */
    public static final class RowsBefore extends ScanRange implements Comparable<RowsBefore> {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier rowIdentifier;

        /**
         * Constructs a range matching rows up to but not including the specified row identifier.
         *
         * @param rowIdentifier
         * the maximum row identifier to match.
         */
        public RowsBefore(@Nonnull final RowIdentifier rowIdentifier) {
            this.rowIdentifier = requireNonNull(rowIdentifier, "rowIdentifier");
        }

        /**
         * Returns the minimum row identifier that this range does not match.
         */
        public RowIdentifier getRowIdentifier() {
            return rowIdentifier;
        }

        @Override
        public Range toAccumuloRange() {
            return new Range(null, rowIdentifier.toHadoopText());
        }

        @Override
        public int compareTo(@Nonnull final RowsBefore that) {
            return this.rowIdentifier.compareTo(that.rowIdentifier);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RowsBefore that = (RowsBefore) o;
            return Objects.equals(this.rowIdentifier, that.rowIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowIdentifier);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RowsBefore{");
            sb.append("rowIdentifier=").append(rowIdentifier);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Range matching all rows between a specified minimum and maximum.
     */
    public static final class RowBounds extends ScanRange {
        private static final long serialVersionUID = 1L;
        private final RowIdentifier minimum;
        private final boolean minimumInclusive;
        private final RowIdentifier maximum;
        private final boolean maximumInclusive;

        /**
         * Constructs a range matching rows starting with the specified row identifier.
         *
         * @param minimum
         * the minimum row identifier to match.
         * @param minimumInclusive
         * @{code true} if the range includes the minimum, {@code false} if it excludes the minimum.
         * @param maximum
         * the maximum row identifier to match.
         * @param maximumInclusive
         * @{code true} if the range includes the maximum, {@code false} if it excludes the maximum.
         */
        public RowBounds(
            @Nonnull final RowIdentifier minimum,
            final boolean minimumInclusive,
            @Nonnull final RowIdentifier maximum,
            final boolean maximumInclusive
        ) {
            this.minimum = requireNonNull(minimum, "minimum");
            this.minimumInclusive = minimumInclusive;
            this.maximum = requireNonNull(maximum, "maximum");
            this.maximumInclusive = maximumInclusive;
        }

        /**
         * Returns the minimum row identifier that this range matches.
         */
        public RowIdentifier getMinimum() {
            return minimum;
        }

        /**
         * Returns whether the minimum bound is inclusive or exclusive.
         */
        public boolean isMinimumInclusive() {
            return minimumInclusive;
        }

        /**
         * Returns the maximum row identifier that this range matches.
         */
        public RowIdentifier getMaximum() {
            return maximum;
        }

        /**
         * Returns whether th maximum bound is inclusive or exclusive.
         */
        public boolean isMaximumInclusive() {
            return maximumInclusive;
        }

        @Override
        public Range toAccumuloRange() {
            return new Range(minimum.toHadoopText(), minimumInclusive, maximum.toHadoopText(), maximumInclusive);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RowBounds that = (RowBounds) o;
            return this.minimumInclusive == that.minimumInclusive &&
                this.maximumInclusive == that.maximumInclusive &&
                Objects.equals(this.minimum, that.minimum) &&
                Objects.equals(this.maximum, that.maximum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(minimum, minimumInclusive, maximum, maximumInclusive);
        }
    }

    /**
     * Range matching all entries between a specified minimum and maximum.
     */
    public static final class KeyBounds extends ScanRange {
        private static final long serialVersionUID = 1L;
        private final EntryKey minimum;
        private final boolean minimumInclusive;
        private final EntryKey maximum;
        private final boolean maximumInclusive;

        /**
         * Constructs a range matching rows starting with the specified row identifier.
         *
         * @param minimum
         * the minimum key to match.
         * @param minimumInclusive
         * @{code true} if the range includes the minimum, {@code false} if it excludes the minimum.
         * @param maximum
         * the maximum key to match.
         * @param maximumInclusive
         * @{code true} if the range includes the maximum, {@code false} if it excludes the maximum.
         */
        public KeyBounds(
            @Nonnull final EntryKey minimum,
            final boolean minimumInclusive,
            @Nonnull final EntryKey maximum,
            final boolean maximumInclusive
        ) {
            this.minimum = requireNonNull(minimum, "minimum");
            this.minimumInclusive = minimumInclusive;
            this.maximum = requireNonNull(maximum, "maximum");
            this.maximumInclusive = maximumInclusive;
        }

        /**
         * Returns the minimum key that this range matches.
         */
        public EntryKey getMinimum() {
            return minimum;
        }

        /**
         * Returns whether the minimum bound is inclusive or exclusive.
         */
        public boolean isMinimumInclusive() {
            return minimumInclusive;
        }

        /**
         * Returns the maximum key that this range matches.
         */
        public EntryKey getMaximum() {
            return maximum;
        }

        /**
         * Returns whether th maximum bound is inclusive or exclusive.
         */
        public boolean isMaximumInclusive() {
            return maximumInclusive;
        }

        @Override
        public Range toAccumuloRange() {
            return new Range(minimum.toAccumuloKey(), minimumInclusive, maximum.toAccumuloKey(), maximumInclusive);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final KeyBounds that = (KeyBounds) o;
            return this.minimumInclusive == that.minimumInclusive &&
                this.maximumInclusive == that.maximumInclusive &&
                Objects.equals(this.minimum, that.minimum) &&
                Objects.equals(this.maximum, that.maximum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(minimum, minimumInclusive, maximum, maximumInclusive);
        }
    }
}
