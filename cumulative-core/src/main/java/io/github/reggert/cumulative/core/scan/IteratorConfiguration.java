package io.github.reggert.cumulative.core.scan;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of the configuration for an Accumulo iterator.
 *
 * The main reason this class exists rather than using {@code IteratorSetting} directly to enable the priorities
 * to be automatically assigned at scan time.
 */
public final class IteratorConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final String iteratorClass;
    private final HashMap<String, String> options = new HashMap<>();


    /**
     * Constructs an iterator configuration with no options.
     *
     * @param name
     * the name to assign to the iterator.
     * @param iteratorClass
     * the full name of the iterator class.
     */
    public IteratorConfiguration(
        @Nonnull final String name,
        @Nonnull final String iteratorClass
    ) {
        this.name = requireNonNull(name, "name");
        this.iteratorClass = requireNonNull(iteratorClass, "iteratorClass");
    }


    /**
     * Constructs an iterator configuration with options.
     *
     * @param name
     * the name to assign to the iterator.
     * @param iteratorClass
     * the full name of the iterator class.
     * @param options
     * the configuration options to pass to the iterator.
     */
    public IteratorConfiguration(
        @Nonnull final String name,
        @Nonnull final String iteratorClass,
        @Nonnull final Map<String, String> options
    ) {
        this(name, iteratorClass);
        this.options.putAll(requireNonNull(options, "options"));
    }


    /**
     * Constructs an {@code IteratorConfiguration} using the simple name of the iterator class as the iterator
     * name.
     *
     * @param iteratorClass
     * the class of iterator.
     */
    public IteratorConfiguration(
        @Nonnull final Class<? extends SortedKeyValueIterator> iteratorClass,
        @Nonnull final Map<String, String> options
    ) {
        this(iteratorClass.getSimpleName(), iteratorClass.getName(), options);
    }


    /**
     * Constructs an {@code IteratorConfiguration} using the simple name of the iterator class as the iterator
     * name, and no options.
     *
     * @param iteratorClass
     * the class of iterator.
     */
    public IteratorConfiguration(
        @Nonnull final Class<? extends SortedKeyValueIterator> iteratorClass
    ) {
        this(iteratorClass.getSimpleName(), iteratorClass.getName());
    }


    /**
     * Returns the name of the iterator.
     */
    public String getName() {
        return name;
    }


    /**
     * Returns the name of the class of the iterator.
     */
    public String getIteratorClass() {
        return iteratorClass;
    }


    /**
     * Returns the configuration options to pass to the iterator.
     */
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }


    /**
     * Constructs an Accumulo {@code IteratorSetting} using the settings stored in this object and assigns
     * it the specified priority.
     *
     * @param priority
     * the priority to assign. Iterators with lower priority numbers get applied first.
     * @return a new {@code IteratorSetting}.
     */
    public final IteratorSetting toIteratorSetting(final int priority) {
        return new IteratorSetting(priority, name, iteratorClass, getOptions());
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final IteratorConfiguration that = (IteratorConfiguration) o;
        return Objects.equals(this.name, that.name) &&
            Objects.equals(this.iteratorClass, that.iteratorClass) &&
            Objects.equals(this.options, that.options);
    }


    @Override
    public int hashCode() {
        return Objects.hash(name, iteratorClass, options);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IteratorConfiguration{");
        sb.append("name='").append(name).append('\'');
        sb.append(", iteratorClass='").append(iteratorClass).append('\'');
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }
}
