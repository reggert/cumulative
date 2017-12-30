package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo value.
 */
public final class EntryValue implements Serializable, Comparable<EntryValue> {
    private static final long serialVersionUID = 1L;
    /**
     * An empty value.
     */
    public static final EntryValue EMPTY = new EntryValue(ByteSequence.EMPTY);
    private final ByteSequence byteSequence;


    private EntryValue(@Nonnull final ByteSequence byteSequence) {
        this.byteSequence = byteSequence;
    }


    /**
     * Constructs a {@code EntryValue} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new EntryValue(requireNonNull(byteSequence, "byteSequence"));
    }


    /**
     * Constructs a {@code EntryValue} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromHadoopText(@Nonnull final Text hadoopText) {
        return new EntryValue(ByteSequence.fromHadoopText(hadoopText));
    }


    /**
     * Constructs a {@code EntryValue} from a byte array.
     *
     * @param byteArray
     * the byte array containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromByteArray(@Nonnull final byte[] byteArray) {
        return new EntryValue(ByteSequence.fromByteArray(byteArray));
    }


    /**
     * Constructs a {@code EntryValue} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromString(@Nonnull final String string) {
        return new EntryValue(ByteSequence.fromString(string));
    }


    /**
     * Constructs a {@code EntryValue} from an Accumulo {@code Value} object.
     *
     * @param accumuloValue
     * the value to convert.
     * @return a new {@code EntryValue} object.
     */
    public static EntryValue fromAccumuloValue(@Nonnull final Value accumuloValue) {
        return new EntryValue(ByteSequence.fromAccumuloValue(accumuloValue));
    }


    /**
     * Converts this {@code EntryValue} to a {@link ByteSequence}.
     *
     * @return the byte sequence underlying this entry.
     */
    public ByteSequence toByteSequence() {
        return byteSequence;
    }


    /**
     * Converts this {@code EntryValue} to a byte array.
     *
     * @return a new byte array.
     */
    public byte[] toByteArray() {
        return byteSequence.toByteArray();
    }


    /**
     * This returns the value of the bytes as if encoded as UTF-8 text.
     */
    @Override
    public String toString() {
        return byteSequence.toString();
    }


    @Override
    public int compareTo(@Nonnull final EntryValue that) {
        return this.byteSequence.compareTo(that.byteSequence);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final EntryValue that = (EntryValue) o;
        return this.byteSequence.equals(that.byteSequence);
    }


    @Override
    public int hashCode() {
        return Objects.hash(byteSequence);
    }
}
