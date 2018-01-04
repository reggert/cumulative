package io.github.reggert.cumulative.core.data;


import org.apache.accumulo.core.data.Key;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo timestamp.
 *
 * A timestamp may be either specified or unspecified. Timestamps are typically left unspecified when writing
 * new data to Accumulo, in which case Accumulo will assign the current time when it writes to the row.
 * Timestamps will always be specified when reading from Accumulo.
 */
public abstract class Timestamp implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final Unspecified UNSPECIFIED = new Unspecified();


    private Timestamp() {}


    /**
     * Represents a timestamp that has not been specified.
     *
     * For mutations, this means to apply the current database time as the timestamp.
     */
    public static final class Unspecified extends Timestamp {
        private static final long serialVersionUID = 1L;
        private Unspecified() {}

        @Override
        public String toString() {
            return "UNSPECIFIED";
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass());
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof Unspecified;
        }
    }


    /**
     * Represents a timestamp that has been specified.
     *
     * This may either be an actual time in milliseconds since the Unix Epoch, or a logical time.
     */
    public static final class Specified extends Timestamp implements Comparable<Specified> {
        private static final long serialVersionUID = 1L;
        private final long value;

        /**
         * Constructs a timestamp with the specified value.
         *
         * @param value
         * the value of the timestamp, which is either the number of milliseconds since the Unix Epoch,
         * or a logical time value.
         * @throws IllegalArgumentException if the argument is {@code Long.MAX_VALUE}.
         */
        public Specified(final long value) {
            if (value == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Long.MAX_VALUE is not a valid timestamp");
            }
            this.value = value;
        }

        /**
         * Returns the numeric value of this timestamp.
         */
        public long longValue() {
            return value;
        }

        /**
         * Converts this timestamp to an {@link Instant}, assuming it represents an actual time rather than
         * a logical time.
         *
         * @return a new {@code Instant}.
         */
        public Instant toInstant() {
            return Instant.ofEpochMilli(value);
        }

        @Override
        public int compareTo(@Nonnull final Specified that) {
            return Long.compare(this.value, that.value);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            final Specified that = (Specified) o;
            return this.value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), value);
        }

        @Override
        public String toString() {
            return toInstant().toString();
        }
    }


    /**
     * Constructs a timestamp from an {@code Instant} truncated to the nearest millisecond.
     *
     * @param instant
     * a time from which to create a timestamp.
     * @return a new {@code Timestamp}.
     */
    public static Specified fromInstant(@Nonnull final Instant instant) {
        return new Specified(requireNonNull(instant, "instant").toEpochMilli());
    }


    /**
     * Constructs a {@code Timestamp} by extracting its value from an Accumulo {@code Key}.
     *
     * @param accumuloKey
     * the key from which to extract the timestamp.
     * @return a new {@code Timestamp}
     */
    public static Timestamp fromAccumuloKey(@Nonnull final Key accumuloKey) {
        return accumuloKey.getTimestamp() == Long.MAX_VALUE
            ? UNSPECIFIED : new Specified(accumuloKey.getTimestamp());
    }

}
