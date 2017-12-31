package io.github.reggert.cumulative.core.data;


import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Base class for classes that wrap a {@link ByteSequence}.
 */
public abstract class ByteSequenceWrapper<T extends ByteSequenceWrapper<T>>
    implements Serializable, Comparable<T>
{
    private static final long serialVersionUID = 1L;
    private final ByteSequence byteSequence;


    ByteSequenceWrapper(@Nonnull final ByteSequence byteSequence) {
        this.byteSequence = requireNonNull(byteSequence, "byteSequence");
    }


    ByteSequenceWrapper(@Nonnull final Text hadoopText) {
        this(ByteSequence.fromHadoopText(hadoopText));
    }


    ByteSequenceWrapper(@Nonnull final Value accumuloValue) {
        this(ByteSequence.fromAccumuloValue(accumuloValue));
    }


    ByteSequenceWrapper(@Nonnull final String string) {
        this(ByteSequence.fromString(string));
    }


    ByteSequenceWrapper(@Nonnull final byte[] bytes) {
        this(ByteSequence.fromByteArray(bytes));
    }


    /**
     * Returns the underlying {@link ByteSequence}.
     */
    public final ByteSequence toByteSequence() {
        return byteSequence;
    }


    /**
     * Converts this object to a Hadoop {@code Text} object.
     *
     * @return a new {@code Text} object.
     */
    public final Text toHadoopText() {
        return byteSequence.toHadoopText();
    }


    /**
     * Converts this object to an array of bytes.
     *
     * @return a new byte array.
     */
    public final byte[] toByteArray() {
        return byteSequence.toByteArray();
    }


    /**
     * Converts this object to an Accumulo {@code Value} object.
     *
     * @return a new {@code Value}.
     */
    public final Value toAccumuloValue() {
        return byteSequence.toAccumuloValue();
    }


    /**
     * Converts this object to a {@code String}, assuming its byte are UTF-8 encoded text.
     *
     * @return a new {@code String}.
     */
    @Override
    public final String toString() {
        return byteSequence.toString();
    }


    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ByteSequenceWrapper<?> that = (ByteSequenceWrapper<?>) o;
        return Objects.equals(this.byteSequence, that.byteSequence);
    }


    @Override
    public final int hashCode() {
        return Objects.hash(byteSequence);
    }


    @Override
    public final int compareTo(@Nonnull final T that) {
        return this.byteSequence.compareTo(that.toByteSequence());
    }
}
