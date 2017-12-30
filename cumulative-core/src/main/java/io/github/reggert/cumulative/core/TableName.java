package io.github.reggert.cumulative.core;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;


/**
 * Represents a table name with optional namespace.
 */
public final class TableName implements Serializable, Comparable<TableName> {
    private static final long serialVersionUID = 1L;
    /**
     * Names and namespaces may not contain whitespace or dots.
     */
    public static final Pattern VALID_PART = Pattern.compile("[^\\.\\s]*");
    public static final Pattern VALID_QUALIFIED_NAME = Pattern.compile("(?:(?<ns>[^\\.\\s]+)\\.)?(?<name>[^\\.\\s]+)");
    private static final Comparator<TableName> COMPARATOR =
        comparing(TableName::getNamespace).thenComparing(TableName::getName);
    private final String namespace;
    private final String name;


    private static String checkValidPart(@Nonnull final String part) {
        if (!VALID_PART.matcher(part).matches()) {
            throw new IllegalArgumentException(String.format("'%s' is not a valid table name token", part));
        }
        return part;
    }


    /**
     * Creates a table name by parsing the namespace and name from the specified string.
     *
     * @param qualifiedName
     * a table name, possibly prefixed with a namespace and dot.
     * @return a table name.
     * @throws IllegalArgumentException if the argument is not in the expected format.
     */
    public static TableName parse(@Nonnull final String qualifiedName) {
        final Matcher matcher =
            VALID_QUALIFIED_NAME.matcher(requireNonNull(qualifiedName, "qualifiedName"));
        if (matcher.matches()) {
            final String namespace = matcher.group("ns");
            final String name = matcher.group("name");
            return new TableName(namespace == null ? "" : namespace, name);
        }
        throw new IllegalArgumentException("Invalid qualified table name: " + qualifiedName);
    }


    /**
     * Constructs a table name from a namespace and name.
     *
     * @param namespace
     * Namespace to attach to the table name; may be empty for the default namespace.
     * @param name
     * Name within the namespace; must not be empty.
     * @throws IllegalArgumentException if either argument contains invalid characters or the name is empty.
     */
    public TableName(@Nonnull final String namespace, @Nonnull final String name) {
        this.namespace = checkValidPart(requireNonNull(namespace, "namespace"));
        this.name = checkValidPart(requireNonNull(name, "name"));
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Empty table names are not valid.");
        }
    }


    /**
     * Returns the (possibly empty) namespace.
     */
    public String getNamespace() {
        return namespace;
    }


    /**
     * Returns the name within the namespace.
     */
    public String getName() {
        return name;
    }


    @Override
    public int compareTo(@Nonnull final TableName that) {
        return COMPARATOR.compare(this, that);
    }


    @Override
    public String toString() {
        if (namespace.isEmpty()) {
            return name;
        }
        else {
            return namespace + '.' + name;
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TableName that = (TableName) o;
        return this.namespace.equals(that.namespace) && this.name.equals(that.name);
    }


    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }
}
