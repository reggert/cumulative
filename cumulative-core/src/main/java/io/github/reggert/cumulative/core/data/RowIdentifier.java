package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo rowid.
 */
public final class RowIdentifier implements Serializable, Comparable<RowIdentifier> {
    private final ByteSequence byteSequence;


    private RowIdentifier(@Nonnull final ByteSequence byteSequence) {
        this.byteSequence = byteSequence;
    }


    /**
     * Constructs a {@code RowIdentifier} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new RowIdentifier(requireNonNull(byteSequence, "byteSequence"));
    }


    /**
     * Constructs a {@code RowIdentifier} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromHadoopText(@Nonnull final Text hadoopText) {
        return new RowIdentifier(ByteSequence.fromHadoopText(hadoopText));
    }


    /**
     * Constructs a {@code RowIdentifier} from a byte array.
     *
     * @param byteArray
     * the byte array containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromByteArray(@Nonnull final byte[] byteArray) {
        return new RowIdentifier(ByteSequence.fromByteArray(byteArray));
    }


    /**
     * Constructs a {@code RowIdentifier} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromString(@Nonnull final String string) {
        return new RowIdentifier(ByteSequence.fromString(string));
    }


    /**
     * Extracts the row identifier from an Accumulo {@code Key} object.
     *
     * @param accumuloKey
     * the key from which to extract the row identifier.
     * @return a new {@code RowIdentifier} object.
     */
    public static RowIdentifier fromAccumuloKey(@Nonnull final Key accumuloKey) {
        return RowIdentifier.fromHadoopText(accumuloKey.getRow());
    }


    /**
     * Converts this {@code RowIdentifier} to a {@link ByteSequence}.
     *
     * @return the value of this row identifier.
     */
    public ByteSequence toByteSequence() {
        return byteSequence;
    }


    /**
     * Converts this {@code RowIdentifier} to a byte array.
     *
     * @return a new byte array.
     */
    public byte[] toByteArray() {
        return byteSequence.toByteArray();
    }


    /**
     * This returns the value of the bytes in the identifier as if encoded as UTF-8 text.
     */
    @Override
    public String toString() {
        return byteSequence.toString();
    }


    @Override
    public int compareTo(@Nonnull final RowIdentifier that) {
        return this.byteSequence.compareTo(that.byteSequence);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RowIdentifier that = (RowIdentifier) o;
        return this.byteSequence.equals(that.byteSequence);
    }


    @Override
    public int hashCode() {
        return Objects.hash(byteSequence);
    }
}
