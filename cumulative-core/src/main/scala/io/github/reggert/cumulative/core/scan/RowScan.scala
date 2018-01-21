package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data.Row
import io.github.reggert.cumulative.core.scan.iterators.WholeRow
import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat
import org.apache.hadoop.mapreduce.Job

import scala.collection.immutable


/**
  * Immutable scan against entire Accumulo rows.
  *
  * All scans extend the [[Traversable]] trait. Note that some operations on `Traversable` cause all elements
  * within the range to be read right away. When chaining operations, it is recommended to use the `view`
  * method to create a lazy view of the collection to avoid needlessly materializing the entire scan.
  */
final class RowScan private(entryScan : Scan) extends Serializable with Traversable[Row] {
  override def foreach[U](f: Row => U): Unit = entryScan.foreach(Row.decode)

  /**
    * Configures a Hadoop job configuration for this scan for use with [[AccumuloInputFormat]].
    *
    * Note that the returned data will still need to be converted into instances of `Entry` and decoded using
    * `Row.decode`.
    *
    * @param configuration Hadoop job configuration to which to apply settings.
    */
  def apply(configuration : Job) : Unit = entryScan.configure(configuration)
}


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
  ) : RowScan = new RowScan(Scan.Simple(tableName, range, List(WholeRow()), columns))

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
  ) : RowScan = new RowScan(Scan.Batch(tableName, ranges, List(WholeRow()), columns))
}
