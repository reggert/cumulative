package io.github.reggert.cumulative.core.data;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of an Accumulo visibilty.
 * It does not perform validation on the expressions, however.
 */
public final class EntryVisibility implements Serializable, Comparable<EntryVisibility> {
    /**
     * An empty visibility, making an entry visible to all scanners.
     */
    public static final EntryVisibility DEFAULT = new EntryVisibility(ByteSequence.EMPTY);
    private final ByteSequence byteSequence;


    private EntryVisibility(@Nonnull final ByteSequence byteSequence) {
        this.byteSequence = byteSequence;
    }


    /**
     * Constructs a {@code EntryVisibility} from a {@link ByteSequence}.
     *
     * @param byteSequence
     * byte sequence containing the visiblity.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromByteSequence(@Nonnull final ByteSequence byteSequence) {
        return new EntryVisibility(requireNonNull(byteSequence, "byteSequence"));
    }


    /**
     * Constructs a {@code EntryVisibility} from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * the {@code Text} object containing the visibility.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromHadoopText(@Nonnull final Text hadoopText) {
        return new EntryVisibility(ByteSequence.fromHadoopText(hadoopText));
    }


    /**
     * Constructs a {@code EntryVisibility} from a byte array.
     *
     * @param byteArray
     * the byte array containing the visibility.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromByteArray(@Nonnull final byte[] byteArray) {
        return new EntryVisibility(ByteSequence.fromByteArray(byteArray));
    }


    /**
     * Constructs a {@code EntryVisibility} from a {@code String}.
     *
     * @param string
     * the {@code string} containing the visibility.
     * @return a new {@code EntryVisibility}
     */
    public static EntryVisibility fromString(@Nonnull final String string) {
        return new EntryVisibility(ByteSequence.fromString(string));
    }


    /**
     * Constructs a {@code EntryVisibility} from an Accumulo {@code ColumnVisibility} object.
     *
     * @param columnVisibility
     * the visibility to convert.
     * @return a new {@code EntryVisibility} object.
     */
    public static EntryVisibility fromColumnVisiblity(@Nonnull final ColumnVisibility columnVisibility) {
        return new EntryVisibility(ByteSequence.fromByteArray(columnVisibility.flatten()));
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
     * Converts this {@code EntryVisibility} to a {@link ByteSequence}.
     *
     * @return the byte sequence underlying this entry.
     */
    public ByteSequence toByteSequence() {
        return byteSequence;
    }


    /**
     * Converts this {@code EntryVisibility} to a byte array.
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
    public int compareTo(@Nonnull final EntryVisibility that) {
        return this.byteSequence.compareTo(that.byteSequence);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final EntryVisibility that = (EntryVisibility) o;
        return this.byteSequence.equals(that.byteSequence);
    }


    @Override
    public int hashCode() {
        return Objects.hash(byteSequence);
    }
}
