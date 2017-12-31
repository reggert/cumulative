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
public final class Timestamp implements Serializable, Comparable<Timestamp> {
    private static final long serialVersionUID = 1L;
    private final long value;
    public static final Timestamp UNSPECIFIED = new Timestamp(Long.MAX_VALUE);


    /**
     * Constructs a {@code Timestamp} from a numeric value.
     *
     * @param value
     *   the numeric value of the timestamp. This will either be the number of milliseconds since the Unix
     *   Epoch, a logical time, or {@code Long.MAX_VALUE} to indicate an unspecified value (to tell
     *   Accumulo to use the current time).
     */
    public Timestamp(final long value) {
        this.value = value;
    }


    /**
     * Constructs a timestamp from an {@code Instant} truncated to the nearest millisecond.
     *
     * @param instant
     * a time from which to create a timestamp.
     * @return a new {@code Timestamp}.
     */
    public Timestamp fromInstant(@Nonnull final Instant instant) {
        return new Timestamp(requireNonNull(instant, "instant").toEpochMilli());
    }


    /**
     * Constructs a {@code Timestamp} by extracting its value from an Accumulo {@code Key}.
     *
     * @param accumuloKey
     * the key from which to extract the timestamp.
     * @return a new {@code Timestamp}
     */
    public Timestamp fromAccumuloKey(@Nonnull final Key accumuloKey) {
        return new Timestamp(accumuloKey.getTimestamp());
    }


    /**
     * Returns the numeric value of this timestamp.
     *
     * @return the numeric value of the timestamp, which may be the number of milliseconds since the Unix
     *   Epoch, a logical time, or {@code Long.MAX_VALUE} to indicate an unspecified value.
     */
    public long longValue() {
        return value;
    }


    /**
     * Indicates whether this {@code Timestamp} represents a specific time (logical or clock), or was left
     * unspecified.
     *
     * @return {@code true} if this timestamp represents a specified value, {@code false} if unspecified.
     */
    public boolean isSpecified() {
        return value != Long.MAX_VALUE;
    }


    /**
     * Converts this {@code Timestamp} to an {@code Instant}, assuming its value represents a clock time.
     *
     * @return an {@code Instant} containing the clock time representation of this timestamp.
     * @throws IllegalStateException if this timestamp is unspecified.
     */
    public Instant toInstant() {
        if (isSpecified()) {
            return Instant.ofEpochMilli(value);
        }
        else {
            throw new IllegalStateException("Timestamp is unspecified");
        }
    }


    @Override
    public int compareTo(@Nonnull final Timestamp that) {
        return Long.compare(this.value, that.value);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Timestamp that = (Timestamp) o;
        return this.value == that.value;
    }


    @Override
    public int hashCode() {
        return Objects.hash(value);
    }


    @Override
    public String toString() {
        if (value == Long.MAX_VALUE) {
            return "UNSPECIFIED";
        }
        else {
            return toInstant().toString();
        }
    }
}
