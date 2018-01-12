package io.github.reggert.cumulative.core.scan;


import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

import static java.util.Objects.requireNonNull;


/**
 * Convenience functions to configure built-in iterators.
 */
public final class Iterators {
    private Iterators() {}


    /**
     * Configures an {@link AgeOffFilter} iterator with the specified TTL threshold.
     * This iterator will filter out records older than the threshold.
     *
     * @param threshold
     * the maximum age of an entry before it is aged off.
     * @return an iterator configuration.
     */
    public static IteratorConfiguration ageOffFilter(@Nonnull final Duration threshold) {
        requireNonNull(threshold, "threshold");
        final HashMap<String, String> options = new HashMap<>();
        options.put("ttl", Long.toString(threshold.toMillis()));
        return new IteratorConfiguration(AgeOffFilter.class, options);
    }


    /**
     * Configures an {@link AgeOffFilter} iterator with the specified TTL threshold.
     * This iterator will filter out records older than the threshold.
     *
     * @param threshold
     * the maximum age of an entry before it is aged off.
     * @param currentTime
     * the current time for purposes of age-off.
     * @return an iterator configuration.
     */
    public static IteratorConfiguration ageOffFilter(
        @Nonnull final Duration threshold,
        @Nonnull final Instant currentTime
    ) {
        requireNonNull(threshold, "threshold");
        requireNonNull(currentTime);
        final HashMap<String, String> options = new HashMap<>();
        options.put("ttl", Long.toString(threshold.toMillis()));
        options.put("currentTime", Long.toString(currentTime.toEpochMilli()));
        return new IteratorConfiguration(AgeOffFilter.class, options);
    }


    /**
     * Configures a {@link FirstEntryInRowIterator} using the default scan settings.
     *
     * @return an iterator configuration.
     */
    public static IteratorConfiguration firstEntryInRow() {
        return new IteratorConfiguration(FirstEntryInRowIterator.class);
    }


    /**
     * Configures a {@link FirstEntryInRowIterator} using the specified scan setting.
     *
     * @param scansBeforeSeek
     * The number of scans to try before attempt to seek.
     * @return an iterator configuration.
     */
    public static IteratorConfiguration firstEntryInRow(final int scansBeforeSeek) {
        final HashMap<String, String> options = new HashMap<>();
        options.put("scansBeforeSeek", Integer.toString(scansBeforeSeek));
        return new IteratorConfiguration(FirstEntryInRowIterator.class, options);
    }
}
