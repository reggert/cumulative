package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo column family..
 */
public final class ColumnFamily implements Serializable, Comparable<ColumnFamily> {
    private final ByteSequence byteSequence;


    private ColumnFamily(@Nonnull final ByteSequence byteSequence) {
        this.byteSequence = byteSequence;
    }


    /**
     * Constructs a {@code ColumnFamily} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new ColumnFamily(requireNonNull(byteSequence, "byteSequence"));
    }


    /**
     * Constructs a {@code ColumnFamily} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromHadoopText(@Nonnull final Text hadoopText) {
        return new ColumnFamily(ByteSequence.fromHadoopText(hadoopText));
    }


    /**
     * Constructs a {@code ColumnFamily} from a byte array.
     *
     * @param byteArray
     * the byte array containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromByteArray(@Nonnull final byte[] byteArray) {
        return new ColumnFamily(ByteSequence.fromByteArray(byteArray));
    }


    /**
     * Constructs a {@code ColumnFamily} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromString(@Nonnull final String string) {
        return new ColumnFamily(ByteSequence.fromString(string));
    }


    /**
     * Extracts the column family from an Accumulo {@code Key} object.
     *
     * @param accumuloKey
     * the key from which to extract the row identifier.
     * @return a new {@code ColumnFamily} object.
     */
    public static ColumnFamily fromAccumuloKey(@Nonnull final Key accumuloKey) {
        return ColumnFamily.fromHadoopText(accumuloKey.getColumnFamily());
    }


    /**
     * Converts this {@code ColumnFamily} to a {@link ByteSequence}.
     *
     * @return the value of this column family.
     */
    public ByteSequence toByteSequence() {
        return byteSequence;
    }


    /**
     * Converts this {@code ColumnFamily} to a byte array.
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
    public int compareTo(@Nonnull final ColumnFamily that) {
        return this.byteSequence.compareTo(that.byteSequence);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ColumnFamily that = (ColumnFamily) o;
        return this.byteSequence.equals(that.byteSequence);
    }


    @Override
    public int hashCode() {
        return Objects.hash(byteSequence);
    }
}
