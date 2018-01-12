package io.github.reggert.cumulative.core.scan;

import io.github.reggert.cumulative.core.TableName;
import io.github.reggert.cumulative.core.data.Entry;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.*;


public abstract class AbstractScan<S extends ScannerSettings, X extends ScannerBase> implements Scan<Entry> {
    protected final Connector connector;
    protected final TableName tableName;
    protected final S scannerSettings;
    protected final ArrayList<IteratorConfiguration> iteratorConfigurations;


    AbstractScan(
        @Nonnull final Connector connector,
        @Nonnull final TableName tableName,
        @Nonnull final S scannerSettings,
        @Nonnull final List<IteratorConfiguration> iteratorConfigurations
    ) {
        this.connector = requireNonNull(connector, "connector");
        this.tableName = requireNonNull(tableName, "tableName");
        this.scannerSettings = requireNonNull(scannerSettings, "scannerSettings");
        this.iteratorConfigurations =
            new ArrayList<>(requireNonNull(iteratorConfigurations, "iteratorConfigurations"));
    }


    protected abstract X createScanner() throws TableNotFoundException;


    protected void configureScanner(@Nonnull final X scanner) {
        scannerSettings.getBatchTimeout().ifPresent(t ->
            scanner.setBatchTimeout(t.toMillis(), TimeUnit.MILLISECONDS)
        );
        scannerSettings.getTimeout().ifPresent(t ->
            scanner.setTimeout(t.toMillis(), TimeUnit.MILLISECONDS)
        );
        scannerSettings.getClassloaderContext().ifPresent(scanner::setClassLoaderContext);
        addScanIterators(scanner);
    }


    @Override
    public final <T> T execute(@Nonnull final Function<Stream<Entry>, T> resultsHandler) {
        try (final X scanner = createScanner()) {
            configureScanner(scanner);
            final Stream<Map.Entry<Key, Value>> rawEntries = StreamSupport.stream(
                scanner::spliterator,
                ORDERED | DISTINCT | NONNULL,
                false
            );
            final Stream<Entry> entries = rawEntries.map(Entry::fromAccumuloEntry);
            return resultsHandler.apply(entries);
        } catch (final TableNotFoundException e) {
            throw new ScanException(String.format("Unable to scan nonexistent table '%s'", tableName), e);
        }
    }


    private void addScanIterators(@Nonnull final X scanner) {
        int priority = 0;
        for (final IteratorConfiguration iteratorConfiguration : iteratorConfigurations) {
            scanner.addScanIterator(iteratorConfiguration.toIteratorSetting(priority++));
        }
    }

}
