package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data.Row
import io.github.reggert.cumulative.core.scan.iterators.WholeRow
import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.iterators.user.WholeRowIterator

import scala.collection.immutable


/**
  * Base class for scans against entire Accumulo rows.
  *
  * All scans extend the [[Traversable]] trait. Note that some operations on `Traversable` cause all elements
  * within the range to be read right away. When chaining operations, it is recommended to use the `view`
  * method to create a lazy view of the collection to avoid needlessly materializing the entire scan.
  */
final class RowScan private(entryScan : Scan) extends Serializable with Traversable[Row] {
  override def foreach[U](f: Row => U): Unit = entryScan.foreach {entry =>
    WholeRowIterator.decodeRow(entry.key.toAccumuloKey, entry.value.toAccumuloValue)
  }
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
  def apply(
    tableName : TableName,
    range : ScanRange.WholeRow = ScanRange.FullTable
  ) (implicit
    connectorProvider : ConnectorProvider,
    scannerSettings : ScannerSettings.Simple = ScannerSettings.Simple()
  ) : RowScan = new RowScan(Scan(tableName, range, List(WholeRow())))

  /**
    * Constructs a multi-range unordered scan.
    *
    * @param tableName         table to scan.
    * @param ranges            ranges to include in scan.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param scannerSettings scanner settings.
    * @return a new batch scan.
    */
  def apply(
    tableName : TableName,
    ranges : immutable.Set[_ <: ScanRange.WholeRow]
  ) (implicit
    connectorProvider : ConnectorProvider,
    scannerSettings : ScannerSettings.Batch = ScannerSettings.Batch()
  ) = Scan(tableName, ranges, List(WholeRow()))
}
