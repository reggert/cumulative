package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * A single entry (key-value pair) stored in an Accumulo table.
 */
public final class Entry implements Serializable {
    private static final long serialVersionUID = 1L;
    private final EntryKey key;
    private final EntryValue value;


    /**
     * Constructs an {@code Entry} from the specified key and value.
     *
     * @param key
     * the Accumulo key used to index and sort records.
     * @param value
     * the value of the entry.
     */
    public Entry(@Nonnull final EntryKey key, @Nonnull final EntryValue value) {
        this.key = requireNonNull(key, "key");
        this.value = requireNonNull(value, "value");
    }


    /**
     * Constructs an {@code Entry} by extracting the Accumulo {@code Key} and {@code Value} from the
     * specified {@code Map.Entry}.
     *
     * @param accumuloEntry
     * map entry from Accumulo.
     * @return a new {@code Entry}
     */
    public static Entry fromAccumuloEntry(@Nonnull final Map.Entry<Key, Value> accumuloEntry) {
        return new Entry(
            EntryKey.fromAccumuloKey(accumuloEntry.getKey()),
            EntryValue.fromAccumuloValue(accumuloEntry.getValue())
        );
    }


    /**
     * Returns the key.
     */
    public EntryKey getKey() {
        return key;
    }


    /**
     * Returns the value.
     */
    public EntryValue getValue() {
        return value;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Entry that = (Entry) o;
        return Objects.equals(this.key, that.key) && Objects.equals(this.value, that.value);
    }


    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }


    @Override
    public String toString() {
        return key + " -> " + value;
    }
}
