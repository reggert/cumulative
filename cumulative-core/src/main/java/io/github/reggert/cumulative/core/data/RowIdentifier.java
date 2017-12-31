package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;


/**
 * Immutable representation of an Accumulo rowid.
 */
public final class RowIdentifier extends ByteSequenceWrapper<RowIdentifier> {
    private static final long serialVersionUID = 1L;


    private RowIdentifier(@Nonnull final ByteSequence byteSequence) {
        super(byteSequence);
    }


    private RowIdentifier(@Nonnull Text hadoopText) {
        super(hadoopText);
    }


    private RowIdentifier(@Nonnull String string) {
        super(string);
    }


    private RowIdentifier(@Nonnull byte[] bytes) {
        super(bytes);
    }


    /**
     * Constructs a {@code RowIdentifier} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new RowIdentifier(byteSequence);
    }


    /**
     * Constructs a {@code RowIdentifier} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromHadoopText(@Nonnull final Text hadoopText) {
        return new RowIdentifier(hadoopText);
    }


    /**
     * Constructs a {@code RowIdentifier} from a byte array.
     *
     * @param byteArray
     * the byte array containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromByteArray(@Nonnull final byte[] byteArray) {
        return new RowIdentifier(byteArray);
    }


    /**
     * Constructs a {@code RowIdentifier} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the id value.
     * @return a new {@code RowIdentifier}
     */
    public static RowIdentifier fromString(@Nonnull final String string) {
        return new RowIdentifier(string);
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

}
