package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data.Row
import io.github.reggert.cumulative.core.scan.iterators.{IteratorConfiguration, WholeRow}
import io.github.reggert.cumulative.core.{ConnectorProvider, HadoopJobConfigurer, TableName}
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat
import org.apache.hadoop.mapreduce.Job
import resource.ManagedResource

import scala.collection.immutable


/**
  * Immutable scan against entire Accumulo rows.
  */
final class RowScan private(entryScan : Scan) extends HadoopJobConfigurer {

  /**
    * Creates a [[ManagedResource]] encapsulating the scan results.
    *
    * @return a [[ManagedResource]] managing an iterator over returned rows.
    */
  def results : ManagedResource[Iterator[Row]] = entryScan.results.map(_.map(Row.decode))

  /**
    * Configures a Hadoop job configuration for this scan for use with [[AccumuloInputFormat]].
    *
    * Note that the returned data will still need to be converted into instances of `Entry` and decoded using
    * `Row.decode`.
    *
    * @param configuration Hadoop job configuration to which to apply settings.
    */
  override def configure(configuration : Job) : Unit = entryScan.configure(configuration)
}


/**
  * Factory for scans against entire Accumulo rows.
  */
object RowScan {
  /**
    * Constructs a single-range in-order scan.
    *
    * @param tableName table to scan.
    * @param range range to include in scan.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param scannerSettings scanner settings.
    * @return a new simple row scan.
    */
  def Simple(
    tableName : TableName,
    range : ScanRange.WholeRow = ScanRange.FullTable,
    columns : immutable.Set[ColumnSelector] = Set.empty
  ) (implicit
    connectorProvider : ConnectorProvider,
    scannerSettings : ScannerSettings.Simple = ScannerSettings.Simple()
  ) : RowScan = new RowScan(Scan.Simple(tableName, range, Map(IteratorConfiguration.MaxPriority -> WholeRow()), columns))

  /**
    * Constructs a multi-range unordered scan.
    *
    * @param tableName         table to scan.
    * @param ranges            ranges to include in scan.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param scannerSettings scanner settings.
    * @return a new batch scan.
    */
  def Batch(
    tableName : TableName,
    ranges : immutable.Set[_ <: ScanRange.WholeRow],
    columns : immutable.Set[ColumnSelector] = Set.empty
  ) (implicit
    connectorProvider : ConnectorProvider,
    scannerSettings : ScannerSettings.Batch = ScannerSettings.Batch()
  ) : RowScan = new RowScan(Scan.Batch(tableName, ranges, Map(IteratorConfiguration.MaxPriority -> WholeRow()), columns))
}
