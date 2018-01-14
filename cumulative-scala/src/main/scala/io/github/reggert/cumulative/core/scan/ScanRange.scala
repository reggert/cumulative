package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data._
import org.apache.accumulo.core.data.{Range => AccumuloRange}


/**
  * Immutable representation of a key range to use when scanning an Accumulo table.
  */
sealed abstract class ScanRange extends Serializable {
  /**
    * Converts this range to an Accumulo `Range`.
    */
  def toAccumuloRange : AccumuloRange
}


object ScanRange {

  /**
    * Base class for `ScanRange` implementations that operate only on row boundaries.
    */
  sealed abstract class WholeRow extends ScanRange

  /**
    * Class to wrap a value to indicate that whether it is an inclusive or exclusive bound.
    * @tparam T the underlying type.
    */
  sealed abstract class Bound[T] extends Serializable {
    def value : T
    def isInclusive : Boolean
  }
  final case class Inclusive[T](value : T) extends Bound {
    override def isInclusive: Boolean = true
  }
  final case class Exclusive[T](value : T) extends Bound {
    override def isInclusive: Boolean = false
  }
  object Bound {
    def minimumBoundOrdering[T : Ordering] : Ordering[Bound[T]] =
      Ordering.by {
        case Inclusive(x : T) => (x, 0)
        case Exclusive(x : T) => (x, 1)
      }
    def maximumBoundOrdering[T : Ordering] : Ordering[Bound[T]] =
      Ordering.by {
        case Inclusive(x : T) => (x, 1)
        case Exclusive(x : T) => (x, 0)
      }
  }

  /**
    * Range representing a full table scan.
    */
  case object FullTable extends WholeRow {
    override def toAccumuloRange: AccumuloRange = new AccumuloRange
  }

  /**
    * Range matching all rows between two row identifiers.
    *
    * @param minimum starting row identifier.
    * @param maximum ending row identifier.
    */
  final case class RowBounds(
    minimum : Bound[RowIdentifier],
    maximum : Bound[RowIdentifier]
  ) extends WholeRow {
    override def toAccumuloRange: AccumuloRange = new AccumuloRange(
      minimum.value.toHadoopText,
      minimum.isInclusive,
      maximum.value.toHadoopText,
      maximum.isInclusive
    )
  }

  /**
    * Range matching all rows starting from specified minimum.
    *
    * @param minimum starting row identifier.
    */
  final case class MinimumRow(minimum : Bound[RowIdentifier]) extends WholeRow {
    override def toAccumuloRange: AccumuloRange = new AccumuloRange(
      minimum.value.toHadoopText,
      minimum.isInclusive,
      null,
      false
    )
  }
  object MinimumRow {
    private[this] implicit val rowBoundOrdering : Ordering[Bound[RowIdentifier]] =
      Bound.minimumBoundOrdering[RowIdentifier]
    implicit val minimumRowOrdering : Ordering[MinimumRow] = Ordering.by(_.minimum)
  }

  /**
    * Range matching all rows ending with a specified maximum.
    *
    * @param maximum ending row identifier.
    */
  final case class MaximumRow(maximum : Bound[RowIdentifier]) extends WholeRow {
    override def toAccumuloRange: AccumuloRange = new AccumuloRange(
      null,
      false,
      maximum.value.toHadoopText,
      maximum.isInclusive
    )
  }
  object MaximumRow {
    private[this] implicit val rowBoundOrdering : Ordering[Bound[RowIdentifier]] =
      Bound.maximumBoundOrdering[RowIdentifier]
    implicit val maximumRowOrdering : Ordering[MinimumRow] = Ordering.by(_.minimum)
  }

  /**
    * Range matching all entries between two keys.
    *
    * @param minimum starting key.
    * @param maximum ending key.
    */
  final case class KeyBounds(
    minimum : Bound[EntryKey],
    maximum : Bound[EntryKey]
  ) extends ScanRange {
    override def toAccumuloRange: AccumuloRange = new AccumuloRange(
      minimum.value.toAccumuloKey,
      minimum.isInclusive,
      maximum.value.toAccumuloKey,
      maximum.isInclusive
    )
  }

  /**
    * Range matching all rows starting from specified minimum.
    *
    * @param minimum starting row identifier.
    */
  final case class MinimumKey(minimum : Bound[EntryKey]) extends ScanRange {
    override def toAccumuloRange: AccumuloRange = new AccumuloRange(
      minimum.value.toAccumuloKey,
      minimum.isInclusive,
      null,
      false
    )
  }
  object MinimumKey {
    private[this] implicit val keyBoundOrdering : Ordering[Bound[EntryKey]] =
      Bound.minimumBoundOrdering[EntryKey]
    implicit val minimumKeyOrdering : Ordering[MinimumKey] = Ordering.by(_.minimum)
  }

  /**
    * Range matching all rows ending with a specified maximum.
    *
    * @param maximum ending row identifier.
    */
  final case class MaximumKey(maximum : Bound[EntryKey]) extends ScanRange {
    override def toAccumuloRange: AccumuloRange = new AccumuloRange(
      null,
      false,
      maximum.value.toAccumuloKey,
      maximum.isInclusive
    )
  }
  object MaximumKey {
    private[this] implicit val keyBoundOrdering : Ordering[Bound[EntryKey]] =
      Bound.maximumBoundOrdering[EntryKey]
    implicit val maximumKeyOrdering : Ordering[MinimumKey] = Ordering.by(_.minimum)
  }

  /**
    * Range matching all rows with a specified prefix.
    *
    * @param rowPrefix the row prefix to match.
    */
  final case class RowPrefix(rowPrefix : RowIdentifier) extends WholeRow {
    override def toAccumuloRange: AccumuloRange = AccumuloRange.prefix(rowPrefix.toHadoopText)
  }
  object RowPrefix {
    implicit val rowPrefixOrdering : Ordering[RowPrefix] = Ordering.by(_.rowPrefix)
  }

  /**
    * Range matching all entries in the specified row with the specified column family prefix.
    *
    * @param row the row to match.
    * @param columnFamilyPrefix the column family prefix to match.
    */
  final case class ColumnFamilyPrefix(
    row : RowIdentifier,
    columnFamilyPrefix : ColumnFamily
  ) extends ScanRange {
    override def toAccumuloRange: AccumuloRange =
      AccumuloRange.prefix(row.toHadoopText, columnFamilyPrefix.toHadoopText)
  }
  object ColumnFamilyPrefix {
    implicit val columnFamilyPrefixOrdering : Ordering[ColumnFamilyPrefix] =
      Ordering.by(x => (x.row, x.columnFamilyPrefix))
  }

  /**
    * Range matching all entries in the specified row and column family with the specified column qualifier
    * prefix.
    *
    * @param row the row to match.
    * @param columnFamily the column family to match.
    * @param columnQualifierPrefix the column qualifier prefix to match.
    */
  final case class ColumnQualifierPrefix(
    row : RowIdentifier,
    columnFamily: ColumnFamily,
    columnQualifierPrefix : ColumnQualifier
  ) extends ScanRange {
    override def toAccumuloRange: AccumuloRange =
      AccumuloRange.prefix(row.toHadoopText, columnFamily.toHadoopText, columnQualifierPrefix.toHadoopText)
  }
  object ColumnQualifierPrefix {
    implicit val columnQualifierPrefixOrdering : Ordering[ColumnQualifierPrefix] =
      Ordering.by(x => (x.row, x.columnFamily, x.columnQualifierPrefix))
  }

  /**
    * Range matching a specified row in its entirety.
    *
    * @param row the row to match.
    */
  final case class ExactRow(row : RowIdentifier) extends WholeRow {
    override def toAccumuloRange: AccumuloRange = AccumuloRange.exact(row.toHadoopText)
  }
  object ExactRow {
    implicit val exactRowOrdering : Ordering[ExactRow] = Ordering.by(_.row)
  }

  /**
    * Range matching all entries belonging to a specified column family within a specified row.
    *
    * @param row the row to match.
    * @param columnFamily the column family to match.
    */
  final case class ExactColumnFamily(row : RowIdentifier, columnFamily : ColumnFamily) extends ScanRange {
    override def toAccumuloRange: AccumuloRange =
      AccumuloRange.exact(row.toHadoopText, columnFamily.toHadoopText)
  }
  object ExactColumnFamily {
    implicit val exactColumnFamilyOrdering : Ordering[ExactColumnFamily] =
      Ordering.by(x => (x.row, x.columnFamily))
  }

  /**
    * Range matching all entries belonging to a specified column within a specified row.
    *
    * @param row the row to match.
    * @param column the column to match.
    */
  final case class ExactColumn(row : RowIdentifier, column : ColumnIdentifier) extends ScanRange {
    override def toAccumuloRange: AccumuloRange =
      AccumuloRange.exact(row.toHadoopText, column.family.toHadoopText, column.qualifier.toHadoopText)
  }
  object ExactColumn {
    implicit val exactColumnOrdering : Ordering[ExactColumn] =
      Ordering.by(x => (x.row, x.column))
  }
}
