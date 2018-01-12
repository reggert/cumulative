package io.github.reggert.cumulative.core.scan;

import io.github.reggert.cumulative.core.TableName;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;


public final class SimpleScan extends AbstractScan<ScannerSettings.Simple, Scanner> {
    private final ScanRange scanRange;


    public SimpleScan(
        @Nonnull final Connector connector,
        @Nonnull final TableName tableName,
        @Nonnull final ScannerSettings.Simple scannerSettings,
        @Nonnull final List<IteratorConfiguration> iteratorConfigurations,
        @Nonnull final ScanRange scanRange
    ) {
        super(connector, tableName, scannerSettings, iteratorConfigurations);
        this.scanRange = requireNonNull(scanRange, "scanRange");
    }


    @Override
    protected Scanner createScanner() throws TableNotFoundException {
        return connector.createScanner(tableName.toString(), scannerSettings.getAuthorizations());
    }


    @Override
    protected void configureScanner(@Nonnull final Scanner scanner) {
        super.configureScanner(scanner);
        scannerSettings.getBatchSize().ifPresent(scanner::setBatchSize);
        if (scannerSettings.isIsolationEnabled()) {
            scanner.enableIsolation();
        }
        scannerSettings.getReadAheadThreshold().ifPresent(scanner::setReadaheadThreshold);
        scanner.setRange(scanRange.toAccumuloRange());
    }

}
