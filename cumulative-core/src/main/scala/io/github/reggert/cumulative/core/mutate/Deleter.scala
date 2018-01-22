package io.github.reggert.cumulative.core.mutate

import io.github.reggert.cumulative.core.scan.{ColumnSelector, ScanRange, ScannerSettings}
import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.iterators.SortedKeyIterator
import org.apache.accumulo.core.iterators.user.WholeRowIterator

import scala.collection.JavaConverters._

import resource._


/**
  * Immutable and serializable service to perform bulk deletions efficiently.
  *
  * Note that this is equivalent to performing a batch scan using [[SortedKeyIterator]] and creating a
  * mutation for each key returned in order to delete the entry. In some cases it may be more efficient to
  * perform a regular batch scan using [[SortedKeyIterator]] and [[WholeRowIterator]] and deleting
  * each row using a single mutation.
  *
  * @param tableName the table from which to delete.
  * @param connectorProvider provider of Accumulo [[Connector]].
  * @param scannerSettings settings to use when scanning for entries to delete.
  * @param writerSettings settings to use to write mutations.
  */
final class Deleter(val tableName : TableName)(implicit
  val connectorProvider : ConnectorProvider,
  val scannerSettings : ScannerSettings.Batch,
  val writerSettings : WriterSettings
) extends Serializable {
  /**
    * Deletes all entries found within the specified range and (optionally) columns.
    *
    * @param ranges the ranges to delete.
    * @param columns the columns to delete, or empty to delete all columns.
    */
  def apply(ranges : Set[ScanRange], columns : Set[ColumnSelector] = Set.empty) : Unit =
    if (ranges.nonEmpty) {
      managed(
        connectorProvider.connector.createBatchDeleter(
          tableName.toString,
          scannerSettings.authorizations,
          scannerSettings.numberOfQueryThreads,
          writerSettings.toBatchWriterConfig
        )
      ).foreach { batchDeleter =>
        scannerSettings(batchDeleter)
        batchDeleter.setRanges(ranges.map(_.toAccumuloRange).asJavaCollection)
        columns.foreach(_(batchDeleter))
        batchDeleter.delete()
      }
    }
}


object Deleter {
  /**
    * Constructs a new `Deleter`.
    *
    * @param tableName the table from which to delete.
    * @param connectorProvider provider of Accumulo [[Connector]].
    * @param scannerSettings settings to use when scanning for entries to delete.
    * @param writerSettings settings to use to write mutations.
    * @return a new `Deleter`.
    */
  def apply(tableName : TableName)(implicit
    connectorProvider : ConnectorProvider,
    scannerSettings : ScannerSettings.Batch = ScannerSettings.Batch(),
    writerSettings : WriterSettings = WriterSettings()
  ) : Deleter = new Deleter(tableName)
}
