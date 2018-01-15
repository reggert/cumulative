package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data.Entry
import io.github.reggert.cumulative.core.scan.iterators.IteratorConfiguration
import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.{Connector, IteratorSetting, ScannerBase}
import org.apache.hadoop.mapreduce.Job

import scala.collection.JavaConverters._
import scala.collection.immutable


/**
  * Base class for scans against Accumulo entries.
  *
  * All scans extend the [[Traversable]] trait. Note that some operations on `Traversable` cause all elements
  * within the range to be read right away. When chaining operations, it is recommended to use the `view`
  * method to create a lazy view of the collection to avoid needlessly materializing the entire scan.
  */
sealed abstract class Scan extends Serializable with Traversable[Entry] {
  protected def createScanner() : ScannerBase
  def tableName : TableName
  def iterators : immutable.Seq[IteratorConfiguration]
  def connectorProvider : ConnectorProvider
  def scannerSettings : ScannerSettings

  /**
    * Configures a Hadoop job configuration for this scan for use with [[AccumuloInputFormat]].
    *
    * Note that the returned data will still need to be converted into instances of `Entry`.
    *
    * @param configuration Hadoop job configuration to which to apply settings.
    */
  def apply(configuration : Job) : Unit

  final def iteratorSettings : Iterable[IteratorSetting] =
    iterators.view.zipWithIndex.map { case (ic, p) => ic.toIteratorSetting(p) }

  override final def foreach[U](f: Entry => U) : Unit = {
    val scanner = createScanner()
    try {
      scanner.asScala.map(Entry.apply).foreach(f)
    }
    finally {
      scanner.close()
    }
  }
}


object Scan {

  /**
    * Scan implementation for single-range in-order scans.
    *
    * @param tableName table to scan.
    * @param range range to include in scan.
    * @param iterators iterators to apply to the scan.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param scannerSettings scanner settings.
    */
  final class Simple(
    val tableName : TableName,
    val range : ScanRange = ScanRange.FullTable,
    val iterators : immutable.Seq[IteratorConfiguration],
    val columns : immutable.Set[ColumnSelector]
  )(
    implicit val connectorProvider : ConnectorProvider,
    implicit val scannerSettings : ScannerSettings.Simple
  ) extends Scan {
    override protected def createScanner(): ScannerBase = {
      val connector = connectorProvider.connector
      val scanner = connector.createScanner(tableName.toString, scannerSettings.authorizations)
      scannerSettings(scanner)
      columns.foreach(_(scanner))
      iteratorSettings.foreach(scanner.addScanIterator)
      scanner
    }

    override def apply(configuration : Job) : Unit = {
      connectorProvider(configuration)
      scannerSettings(configuration)
      InputFormatBase.setInputTableName(configuration, tableName.toString)
      InputFormatBase.setBatchScan(configuration, false)
      InputFormatBase.setRanges(configuration, Seq(range.toAccumuloRange).asJavaCollection)
      iteratorSettings.foreach(InputFormatBase.addIterator(configuration, _))
    }
  }


  object Simple {
    /**
      * Constructs a single-range in-order scan.
      *
      * @param tableName table to scan.
      * @param range range to include in scan.
      * @param iterators iterators to apply to the scan.
      * @param connectorProvider provider of the Accumulo [[Connector]].
      * @param scannerSettings scanner settings.
      * @return a new simple scan.
      */
    def apply(
      tableName : TableName,
      range : ScanRange = ScanRange.FullTable,
      iterators : immutable.Seq[IteratorConfiguration] = Nil,
      columns : immutable.Set[ColumnSelector] = Set.empty
    ) (implicit
      connectorProvider : ConnectorProvider,
      scannerSettings : ScannerSettings.Simple = ScannerSettings.Simple()
    ) : Scan = new Simple(tableName, range, iterators, columns)
  }


  /**
    * Scanner implementation for batch scans.
    *
    * @param tableName         table to scan.
    * @param ranges            ranges to include in scan.
    * @param iterators         iterators to apply to scan.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param scannerSettings scanner settings.
    */
  final class Batch(
    val tableName : TableName,
    val ranges : immutable.Set[_ <: ScanRange],
    val iterators : immutable.Seq[IteratorConfiguration],
    val columns : immutable.Set[ColumnSelector]
  ) (
    implicit val connectorProvider : ConnectorProvider,
    implicit val scannerSettings : ScannerSettings.Batch
  ) extends Scan {
    override protected def createScanner(): ScannerBase = {
      val connector = connectorProvider.connector
      val scanner = connector.createBatchScanner(
        tableName.toString,
        scannerSettings.authorizations,
        scannerSettings.numberOfQueryThreads
      )
      scannerSettings(scanner)
      iteratorSettings.foreach(scanner.addScanIterator)
      columns.foreach(_(scanner))
      scanner
    }

    override def apply(configuration : Job) : Unit = {
      connectorProvider(configuration)
      scannerSettings(configuration)
      InputFormatBase.setInputTableName(configuration, tableName.toString)
      InputFormatBase.setBatchScan(configuration, true)
      InputFormatBase.setRanges(configuration, ranges.map(_.toAccumuloRange).asJavaCollection)
      iteratorSettings.foreach(InputFormatBase.addIterator(configuration, _))
    }
  }


  object Batch {
    /**
      * Constructs a multi-range unordered scan.
      *
      * @param tableName         table to scan.
      * @param ranges            ranges to include in scan.
      * @param iterators         iterators to apply to scan.
      * @param connectorProvider provider of the Accumulo [[Connector]].
      * @param scannerSettings scanner settings.
      * @return a new batch scan.
      */
    def apply(
      tableName : TableName,
      ranges : immutable.Set[_ <: ScanRange],
      iterators : immutable.Seq[IteratorConfiguration] = Nil,
      columns : immutable.Set[ColumnSelector] = Set.empty
    ) (implicit
      connectorProvider : ConnectorProvider,
      scannerSettings : ScannerSettings.Batch = ScannerSettings.Batch()
    ) : Scan = new Batch(tableName, ranges, iterators, columns)
  }
}
