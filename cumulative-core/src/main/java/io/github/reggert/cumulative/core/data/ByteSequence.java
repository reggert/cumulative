package io.github.reggert.cumulative.core.data;


import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;


/**
 * An immutable sequence of bytes. Used as the internal value class for Accumulo key and value elements.
 */
public final class ByteSequence implements Serializable, Comparable<ByteSequence> {
    public static final ByteSequence EMPTY = new ByteSequence(new byte[0]);
    private static final long serialVersionUID = 1L;
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final byte[] bytes;


    private ByteSequence(@Nonnull final byte[] bytes) {
        this.bytes = requireNonNull(bytes, "bytes");
    }


    /**
     * Constructs a {@code ByteSequence} by copying the specified byte array.
     *
     * @param bytes
     * the byte array to copy.
     */
    public static ByteSequence fromByteArray(@Nonnull final byte[] bytes) {
        return new ByteSequence(Arrays.copyOf(bytes, bytes.length));
    }


    /**
     * Constructs a {@code ByteSequence} by copying the bytes from a Hadoop {@code Text} object.
     *
     * @param hadoopText
     * Hadoop {@code Text} to copy.
     * @return a new {@code ByteSequence}
     */
    public static ByteSequence fromHadoopText(@Nonnull final Text hadoopText) {
        return new ByteSequence(requireNonNull(hadoopText, "hadoopText").copyBytes());
    }


    /**
     * Constructs a {@code ByteSequence} by encoding a {@code String} in UTF-8.
     *
     * @param string
     * @return a new {@code ByteSequence}
     */
    public static ByteSequence fromString(@Nonnull final String string) {
        return new ByteSequence(requireNonNull(string, "string").getBytes(UTF8));
    }


    /**
     * Constructs a {@code ByteSequence} by copying the bytes from an Accumulo {@code Value} object.
     *
     * @param accumuloValue
     * @return a new {@code ByteSequence}
     */
    public static ByteSequence fromAccumuloValue(@Nonnull final Value accumuloValue) {
        return fromByteArray(requireNonNull(accumuloValue, "accumuloValue").get());
    }


    /**
     * Constructs a {@code ByteSequence} by consuming the remaining bytes from a {@code ByteBuffer}.
     * @param byteBuffer
     * the buffer from which to consume bytes.
     * @return a new {@code ByteSequence}
     */
    public static ByteSequence fromByteBuffer(@Nonnull final ByteBuffer byteBuffer) {
        requireNonNull(byteBuffer, "byteBuffer");
        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new ByteSequence(bytes);
    }


    /**
     * Converts the {@code ByteSequence} to a {@code String} by decoding the bytes as UTF-8.
     *
     * @return a String representation of the UTF-8 encoded bytes. Note that this may contain invalid
     * characters if the source data was not UTF-8.
     */
    @Override
    public String toString() {
        return UTF8.decode(ByteBuffer.wrap(bytes)).toString();
    }


    /**
     * Converts this byte sequence to a byte array.
     *
     * @return a new byte array.
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(bytes, bytes.length);
    }


    /**
     * Converts this byte sequence to a Hadoop {@code Text} object.
     *
     * @return a new {@code Text} object.
     */
    public Text toHadoopText() {
        return new Text(toByteArray());
    }


    /**
     * Converts this byte sequence to an Accumulo {@code Value} object.
     *
     * @return a new {@code Value} object.
     */
    public Value toAccumuloValue() {
        return new Value(toByteArray());
    }


    /**
     * Sorts lexicographically, based on the unsigned value of each byte.
     */
    @Override
    public int compareTo(@Nonnull final ByteSequence that) {
        int i = 0, j = 0;
        while (i < this.bytes.length && j < that.bytes.length) {
            final int cmp = (this.bytes[i] & 0xff) - (this.bytes[j] & 0xff);
            if (cmp != 0) {
                return cmp;
            }
        }
        if (i < this.bytes.length) {
            return 1;
        }
        if (j < this.bytes.length) {
            return -1;
        }
        return 0;
    }
}
