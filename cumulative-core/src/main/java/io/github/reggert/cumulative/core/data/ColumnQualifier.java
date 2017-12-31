package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;


/**
 * Immutable representation of an Accumulo column qualifier..
 */
public final class ColumnQualifier extends ByteSequenceWrapper<ColumnQualifier> {
    private static final long serialVersionUID = 1L;
    /**
     * An empty qualifier. Typically indicates that the qualifier is an unused part of the key.
     */
    public static final ColumnQualifier EMPTY = new ColumnQualifier(ByteSequence.EMPTY);


    private ColumnQualifier(@Nonnull final ByteSequence byteSequence) {
        super(byteSequence);
    }


    private ColumnQualifier(@Nonnull Text hadoopText) {
        super(hadoopText);
    }


    private ColumnQualifier(@Nonnull String string) {
        super(string);
    }


    private ColumnQualifier(@Nonnull byte[] bytes) {
        super(bytes);
    }


    /**
     * Constructs a {@code ColumnQualifier} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new ColumnQualifier(byteSequence);
    }


    /**
     * Constructs a {@code ColumnQualifier} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromHadoopText(@Nonnull final Text hadoopText) {
        return new ColumnQualifier(hadoopText);
    }


    /**
     * Constructs a {@code ColumnQualifier} from a byte array.
     *
     * @param byteArray
     * the byte array containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromByteArray(@Nonnull final byte[] byteArray) {
        return new ColumnQualifier(byteArray);
    }


    /**
     * Constructs a {@code ColumnQualifier} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the id value.
     * @return a new {@code ColumnQualifier}
     */
    public static ColumnQualifier fromString(@Nonnull final String string) {
        return new ColumnQualifier(string);
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

}
