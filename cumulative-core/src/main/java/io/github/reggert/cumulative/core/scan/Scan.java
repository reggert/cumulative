package io.github.reggert.cumulative.core.scan;

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.stream.Stream;


public interface Scan<E> {
    <T> T execute(@Nonnull Function<Stream<E>, T> resultsHandler);
}
