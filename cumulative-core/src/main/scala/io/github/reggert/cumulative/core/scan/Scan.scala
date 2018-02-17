package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data.Entry
import io.github.reggert.cumulative.core.scan.iterators.IteratorConfiguration
import io.github.reggert.cumulative.core.{ConnectorProvider, HadoopJobConfigurer, TableName}
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.{Connector, IteratorSetting, ScannerBase}
import org.apache.hadoop.mapreduce.Job

import scala.collection.JavaConverters._
import scala.collection.immutable

import resource._


/**
  * Base class for scans against Accumulo entries.
  *
  * All scans extend the [[Traversable]] trait. Note that some operations on `Traversable` cause all elements
  * within the range to be read right away. When chaining operations, it is recommended to use the `view`
  * method to create a lazy view of the collection to avoid needlessly materializing the entire scan.
  */
sealed abstract class Scan extends HadoopJobConfigurer with Traversable[Entry] {
  /**
    * Implemented by subclasses to construct the appropriate type of scanner.
    * @return a scanner wrapped in [[ManagedResource]].
    */
  protected def createScanner() : ManagedResource[ScannerBase]

  /**
    * The name of the table to be scanned.
    */
  val tableName : TableName

  /**
    * The configurations for the server-side iterators to use in the scan.
    */
  val iterators : immutable.Seq[IteratorConfiguration]

  /**
    * Provider of the Accumulo `Connector` to be used in the scan.
    */
  val connectorProvider : ConnectorProvider

  /**
    * Settings to pass to the scanner.
    */
  val scannerSettings : ScannerSettings

  /**
    * Converts the `iterators` to a collection of [[IteratorSetting]] objects with assigned
    * priorities.
    *
    * @return a collection of [[IteratorSetting]] objects, where each is assigned a priority
    *         based on its index in `iterators`.
    */
  final def iteratorSettings : Iterable[IteratorSetting] =
    iterators.view.zipWithIndex.map { case (ic, p) => ic.toIteratorSetting(p) }

  /**
    * This implementation of `foreach` performs the scan.
    *
    * @param f callback to invoke for each entry returned by Accumulo.
    * @tparam U return type of the callback.
    */
  override final def foreach[U](f: Entry => U) : Unit =
    createScanner().foreach {scanner =>
      scanner.asScala.map(Entry.apply).foreach(f)
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
    override protected def createScanner(): ManagedResource[ScannerBase] = {
      val connector = connectorProvider.connector
      managed(connector.createScanner(tableName.toString, scannerSettings.authorizations)).map { scanner =>
        scannerSettings(scanner)
        columns.foreach(_ (scanner))
        iteratorSettings.foreach(scanner.addScanIterator)
        scanner.setRange(range.toAccumuloRange)
        scanner
      }
    }

    override def configure(configuration : Job) : Unit = {
      connectorProvider.configure(configuration)
      scannerSettings(configuration)
      InputFormatBase.setInputTableName(configuration, tableName.toString)
      InputFormatBase.setBatchScan(configuration, false)
      InputFormatBase.setRanges(configuration, Seq(range.toAccumuloRange).asJavaCollection)
      iteratorSettings.foreach(InputFormatBase.addIterator(configuration, _))
    }
  }


  /**
    * Factory for simple scans.
    */
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
    override protected def createScanner(): ManagedResource[ScannerBase] = {
      val connector = connectorProvider.connector
      managed(
        connector.createBatchScanner(
          tableName.toString,
          scannerSettings.authorizations,
          scannerSettings.numberOfQueryThreads
        )
      ).map { scanner =>
        scannerSettings(scanner)
        iteratorSettings.foreach(scanner.addScanIterator)
        columns.foreach(_ (scanner))
        scanner.setRanges(ranges.view.map(_.toAccumuloRange).asJavaCollection)
        scanner
      }
    }

    override def configure(configuration : Job) : Unit = {
      connectorProvider.configure(configuration)
      scannerSettings(configuration)
      InputFormatBase.setInputTableName(configuration, tableName.toString)
      InputFormatBase.setBatchScan(configuration, true)
      InputFormatBase.setRanges(configuration, ranges.view.map(_.toAccumuloRange).asJavaCollection)
      iteratorSettings.foreach(InputFormatBase.addIterator(configuration, _))
    }
  }


  /**
    * Factory for batch scans.
    */
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
