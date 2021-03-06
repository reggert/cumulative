package io.github.reggert.cumulative.model.scan

import io.github.reggert.cumulative.core.data.{Entry, Row}
import io.github.reggert.cumulative.core.scan.{RowScan, Scan, ScanRange, ScannerSettings}
import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import io.github.reggert.cumulative.model.{EntryModel, RecordModel, RowModel}
import org.apache.accumulo.core.client.Connector
import resource.ManagedResource

import scala.collection.immutable
import scala.util.Try


/**
  * Base class for factories of scans that apply a record model to the results.
  *
  * Failure to translate a record will always result in an exception, which, in cases where
  * failure to translate is expected to occur frequently, may be expensive due to the cost of building stack
  * traces. In such cases, it may be preferable to define the record type as `Either[D, R]`, where it returns
  * a `Left[D]` on success and a `Right[R]` on failure, to report the untranslatable raw values without
  * throwing exceptions.
  *
  * @param model             record model to apply.
  * @param tableName         table to scan.
  * @param connectorProvider provider of Accumulo [[Connector]].
  * @tparam D the domain object type.
  * @tparam R the raw Accumulo type (either [[Entry]] or ([[Row]])
  * @tparam B the base type of scan ranges allowed by the model.
  */
sealed abstract class ModelScanner[D, R, B <: ScanRange] protected(
  val model : RecordModel[D, R, B],
  val tableName : TableName
)(implicit connectorProvider: ConnectorProvider) extends Serializable {
  /**
    * Scans the specified range of records and returns them.
    *
    * If an error occurs when translating a record, the exception thrown by the model is allowed to propagate.
    *
    * @param range the record range to scan.
    * @param scannerSettings settings to pass to the underlying raw scanner.
    * @return a lazy collection of records returned from Accumulo.
    */
  final def scan(range : model.RecordRange = model.FullTable)
    (implicit scannerSettings: ScannerSettings.Simple = ScannerSettings.Simple()) : ManagedResource[Iterator[D]] =
    tryScan(range).map(_.map {case (_, d) => d.get})

  /**
    * Scans the specified ranges of records and returns them.
    *
    * If an error occurs when translating a record, the exception thrown by the model is allowed to propagate.
    *
    * @param ranges the record ranges to scan.
    * @param scannerSettings settings to pass to the underlying raw scanner.
    * @return a lazy collection of records returned from Accumulo.
    */
  final def scanBatch(ranges : immutable.Set[model.RecordRange])
    (implicit scannerSettings: ScannerSettings.Batch = ScannerSettings.Batch()) : ManagedResource[Iterator[D]] =
    tryScanBatch(ranges).map(_.map {case (_, d) => d.get})

  /**
    * Scans the specified range of records and returns the results of translating them along with the raw
    * values from Accumulo.
    *
    * @param range the record range to scan.
    * @param scannerSettings settings to pass to the underlying raw scanner.
    * @return a lazy collection of pairs, where the left side of each pair is the raw datum returned from
    *         Accumulo, and the right side is the result of attempting to translate it into a domain record.
    */
  final def tryScan(range : model.RecordRange = model.FullTable)
    (implicit scannerSettings: ScannerSettings.Simple = ScannerSettings.Simple()) : ManagedResource[Iterator[(R, Try[D])]] =
    rawScan(range.toScanRange).map(_.map {raw => raw -> Try(model.domainFromRaw(raw))})

  /**
    * Scans the specified ranges of records and returns the results of translating them along with the raw
    * values from Accumulo.
    *
    * @param ranges the record ranges to scan.
    * @param scannerSettings settings to pass to the underlying raw scanner.
    * @return a lazy collection of pairs, where the left side of each pair is the raw datum returned from
    *         Accumulo, and the right side is the result of attempting to translate it into a domain record.
    */
  final def tryScanBatch(ranges : immutable.Set[model.RecordRange])
    (implicit scannerSettings: ScannerSettings.Batch = ScannerSettings.Batch()) : ManagedResource[Iterator[(R, Try[D])]] =
    rawScan(ranges.map(_.toScanRange)).map(_.map {raw => raw -> Try(model.domainFromRaw(raw))})

  /**
    * Implemented by subclasses to perform the underlying raw (untranslated scan).
    *
    * @param scanRange the raw range to scan.
    * @param scannerSettings settings to pass to the underlying raw scanner.
    * @return lazy collection containing raw scan results.
    */
  protected def rawScan(scanRange : B)
    (implicit scannerSettings: ScannerSettings.Simple) : ManagedResource[Iterator[R]]

  /**
    * Implemented by subclasses to perform the underlying raw (untranslated scan).
    *
    * @param scanRanges the raw ranges to scan.
    * @param scannerSettings settings to pass to the underlying raw scanner.
    * @return lazy collection containing raw scan results.
    */
  protected def rawScan(scanRanges : immutable.Set[_ <: B])
    (implicit scannerSettings: ScannerSettings.Batch) : ManagedResource[Iterator[R]]
}


/**
  * Scanner that applies an [[EntryModel]] to the results.
  *
  * @param model             record model to apply.
  * @param tableName         table to scan.
  * @param connectorProvider provider of Accumulo [[Connector]].
  * @tparam D the domain object type.
  */
final class EntryModelScanner[D](
  model : EntryModel[D],
  tableName : TableName
)(implicit connectorProvider: ConnectorProvider)
  extends ModelScanner[D, Entry, ScanRange](model, tableName)
{
  override protected def rawScan(scanRange: ScanRange)
    (implicit scannerSettings: ScannerSettings.Simple): ManagedResource[Iterator[Entry]] =
    Scan.Simple(tableName, scanRange, columns = model.columnSelectors).results

  override protected def rawScan(scanRanges: Set[_ <: ScanRange])
    (implicit scannerSettings: ScannerSettings.Batch): ManagedResource[Iterator[Entry]] =
    Scan.Batch(tableName, scanRanges, columns = model.columnSelectors).results
}


/**
  * Scanner that applies a [[RowModel]] to the results.
  *
  * @param model             record model to apply.
  * @param tableName         table to scan.
  * @param connectorProvider provider of Accumulo [[Connector]].
  * @tparam D the domain object type.
  */
final class RowModelScanner[D](
  model : RowModel[D],
  tableName : TableName
) (implicit connectorProvider: ConnectorProvider)
  extends ModelScanner[D, Row, ScanRange.WholeRow](model, tableName)
{
  override protected def rawScan(scanRange: ScanRange.WholeRow)
    (implicit scannerSettings: ScannerSettings.Simple): ManagedResource[Iterator[Row]] =
    RowScan.Simple(tableName, scanRange, columns = model.columnSelectors).results

  override protected def rawScan(scanRanges: Set[_ <: ScanRange.WholeRow])
    (implicit scannerSettings: ScannerSettings.Batch): ManagedResource[Iterator[Row]] =
    RowScan.Batch(tableName, scanRanges, columns = model.columnSelectors).results
}
