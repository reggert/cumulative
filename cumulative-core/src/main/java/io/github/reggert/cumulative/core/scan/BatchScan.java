package io.github.reggert.cumulative.core.scan;

import io.github.reggert.cumulative.core.TableName;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


public final class BatchScan extends AbstractScan<ScannerSettings.Batch, BatchScanner> {
    private final LinkedHashSet<ScanRange> scanRanges;


    public BatchScan(
        @Nonnull final Connector connector,
        @Nonnull final TableName tableName,
        @Nonnull final ScannerSettings.Batch scannerSettings,
        @Nonnull final List<IteratorConfiguration> iteratorConfigurations,
        @Nonnull final Set<ScanRange> scanRanges
    ) {
        super(connector, tableName, scannerSettings, iteratorConfigurations);
        this.scanRanges = new LinkedHashSet<>(requireNonNull(scanRanges, "scanRanges"));
    }


    @Override
    protected BatchScanner createScanner() throws TableNotFoundException {
        return connector.createBatchScanner(
            tableName.toString(),
            scannerSettings.getAuthorizations(),
            scannerSettings.getNumberOfQueryThreads()
        );
    }


    @Override
    protected void configureScanner(@Nonnull final BatchScanner scanner) {
        super.configureScanner(scanner);
        scanner.setRanges(
            scanRanges.stream()
                .map(ScanRange::toAccumuloRange)
                .collect(Collectors.toSet())
        );
    }

}
