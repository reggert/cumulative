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
public final class EntryValue extends ByteSequenceWrapper<EntryValue> {
    private static final long serialVersionUID = 1L;
    /**
     * An empty value.
     */
    public static final EntryValue EMPTY = new EntryValue(ByteSequence.EMPTY);


    private EntryValue(@Nonnull final ByteSequence byteSequence) {
        super(byteSequence);
    }


    private EntryValue(@Nonnull Text hadoopText) {
        super(hadoopText);
    }


    private EntryValue(@Nonnull Value accumuloValue) {
        super(accumuloValue);
    }


    private EntryValue(@Nonnull String string) {
        super(string);
    }


    private EntryValue(@Nonnull byte[] bytes) {
        super(bytes);
    }


    /**
     * Constructs a {@code EntryValue} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new EntryValue(byteSequence);
    }


    /**
     * Constructs a {@code EntryValue} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromHadoopText(@Nonnull final Text hadoopText) {
        return new EntryValue(hadoopText);
    }


    /**
     * Constructs a {@code EntryValue} from a byte array.
     *
     * @param byteArray
     * the byte array containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromByteArray(@Nonnull final byte[] byteArray) {
        return new EntryValue(byteArray);
    }


    /**
     * Constructs a {@code EntryValue} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the value.
     * @return a new {@code EntryValue}
     */
    public static EntryValue fromString(@Nonnull final String string) {
        return new EntryValue(string);
    }


    /**
     * Constructs a {@code EntryValue} from an Accumulo {@code Value} object.
     *
     * @param accumuloValue
     * the value to convert.
     * @return a new {@code EntryValue} object.
     */
    public static EntryValue fromAccumuloValue(@Nonnull final Value accumuloValue) {
        return new EntryValue(accumuloValue);
    }

}
