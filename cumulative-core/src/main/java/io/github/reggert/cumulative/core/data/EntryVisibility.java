package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo visibilty.
 * It does not perform validation on the expressions, however.
 */
public final class EntryVisibility extends ByteSequenceWrapper<EntryVisibility> {
    private static final long serialVersionUID = 1L;
    /**
     * An empty visibility, making an entry visible to all scanners.
     */
    public static final EntryVisibility DEFAULT = new EntryVisibility(ByteSequence.EMPTY);


    private EntryVisibility(@Nonnull final ByteSequence byteSequence) {
        super(byteSequence);
    }


    private EntryVisibility(@Nonnull Text hadoopText) {
        super(hadoopText);
    }


    private EntryVisibility(@Nonnull String string) {
        super(string);
    }


    private EntryVisibility(@Nonnull byte[] bytes) {
        super(bytes);
    }


    /**
     * Constructs a {@code EntryVisibility} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the visiblity.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new EntryVisibility(byteSequence);
    }


    /**
     * Constructs a {@code EntryVisibility} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the visibility.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromHadoopText(@Nonnull final Text hadoopText) {
        return new EntryVisibility(hadoopText);
    }


    /**
     * Constructs a {@code EntryVisibility} from a byte array.
     *
     * @param byteArray
     * the byte array containing the visibility.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromByteArray(@Nonnull final byte[] byteArray) {
        return new EntryVisibility(byteArray);
    }


    /**
     * Constructs a {@code EntryVisibility} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the visibility.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromString(@Nonnull final String string) {
        return new EntryVisibility(string);
    }


    /**
     * Constructs a {@code EntryVisibility} from an Accumulo {@code ColumnVisibility} object.
     *
     * @param columnVisibility
     * the visibility to convert.
     * @return a new {@code EntryVisibility} object.
     */
    public static EntryVisibility fromColumnVisiblity(@Nonnull final ColumnVisibility columnVisibility) {
        return new EntryVisibility(columnVisibility.flatten());
    }


    /**
     * Extracts the visibility from an Accumulo {@code Key} object.
     *
     * @param accumuloKey
     * the key from which to extract the visibility.
     * @return a new {@code ColumnQualifier} object.
     */
    public static EntryVisibility fromAccumuloKey(@Nonnull final Key accumuloKey) {
        return EntryVisibility.fromHadoopText(accumuloKey.getColumnVisibility());
    }


    /**
     * Parses this value into an Accumulo {@code ColumnVisibility}.
     *
     * @return a new {@code ColumnVisibility}.
     * @throws BadArgumentException if the expression is invalid.
     */
    public ColumnVisibility toColumnVisibility() {
        return new ColumnVisibility(toByteArray());
    }

}
