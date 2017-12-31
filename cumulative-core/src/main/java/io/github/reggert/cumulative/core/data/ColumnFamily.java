package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;


/**
 * Immutable representation of an Accumulo column family..
 */
public final class ColumnFamily extends ByteSequenceWrapper<ColumnFamily> {
    private static final long serialVersionUID = 1L;
    /**
     * An empty family name. Typically indicates that the family is an unused part of the key.
     */
    public static final ColumnFamily EMPTY = new ColumnFamily(ByteSequence.EMPTY);


    private ColumnFamily(@Nonnull final ByteSequence byteSequence) {
        super(byteSequence);
    }


    private ColumnFamily(@Nonnull Text hadoopText) {
        super(hadoopText);
    }


    private ColumnFamily(@Nonnull String string) {
        super(string);
    }

    private ColumnFamily(@Nonnull byte[] bytes) {
        super(bytes);
    }


    /**
     * Constructs a {@code ColumnFamily} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new ColumnFamily(byteSequence);
    }


    /**
     * Constructs a {@code ColumnFamily} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromHadoopText(@Nonnull final Text hadoopText) {
        return new ColumnFamily(hadoopText);
    }


    /**
     * Constructs a {@code ColumnFamily} from a byte array.
     *
     * @param byteArray
     * the byte array containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromByteArray(@Nonnull final byte[] byteArray) {
        return new ColumnFamily(byteArray);
    }


    /**
     * Constructs a {@code ColumnFamily} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the id value.
     * @return a new {@code ColumnFamily}
     */
    public static ColumnFamily fromString(@Nonnull final String string) {
        return new ColumnFamily(string);
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
}
