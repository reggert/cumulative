package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo column qualifier..
 */
public final class ColumnQualifier implements Serializable, Comparable<ColumnQualifier> {
    /**
     * An empty qualifier. Typically indicates that the qualifier is an unused part of the key.
     */
    public static final ColumnQualifier EMPTY = new ColumnQualifier(ByteSequence.EMPTY);
    private final ByteSequence byteSequence;


    private ColumnQualifier(@Nonnull final ByteSequence byteSequence) {
        this.byteSequence = byteSequence;
    }


    /**
     * Constructs a {@code ColumnQualifier} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new ColumnQualifier(requireNonNull(byteSequence, "byteSequence"));
    }


    /**
     * Constructs a {@code ColumnQualifier} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromHadoopText(@Nonnull final Text hadoopText) {
        return new ColumnQualifier(ByteSequence.fromHadoopText(hadoopText));
    }


    /**
     * Constructs a {@code ColumnQualifier} from a byte array.
     *
     * @param byteArray
     * the byte array containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromByteArray(@Nonnull final byte[] byteArray) {
        return new ColumnQualifier(ByteSequence.fromByteArray(byteArray));
    }


    /**
     * Constructs a {@code ColumnQualifier} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromString(@Nonnull final String string) {
        return new ColumnQualifier(ByteSequence.fromString(string));
    }


    /**
     * Extracts the column qualifier from an Accumulo {@code Key} object.
     *
     * @param accumuloKey
     * the key from which to extract the column qualifier.
     * @return a new {@code ColumnQualifier} object.
     */
    public static ColumnQualifier fromAccumuloKey(@Nonnull final Key accumuloKey) {
        return ColumnQualifier.fromHadoopText(accumuloKey.getColumnQualifier());
    }


    /**
     * Converts this {@code ColumnQualifier} to a {@link ByteSequence}.
     *
     * @return the value of this column qualifier.
     */
    public ByteSequence toByteSequence() {
        return byteSequence;
    }


    /**
     * Converts this {@code ColumnQualifier} to a byte array.
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
    public int compareTo(@Nonnull final ColumnQualifier that) {
        return this.byteSequence.compareTo(that.byteSequence);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ColumnQualifier that = (ColumnQualifier) o;
        return this.byteSequence.equals(that.byteSequence);
    }


    @Override
    public int hashCode() {
        return Objects.hash(byteSequence);
    }
}
