package io.github.reggert.cumulative.core.mutate

import io.github.reggert.cumulative.core.data.{ColumnIdentifier, EntryValue, EntryVisibility, Timestamp}
import io.github.reggert.cumulative.core.scan.iterators.IteratorConfiguration
import org.apache.accumulo.core.data.{Condition, ConditionalMutation}

import scala.collection.immutable


/**
  * Base class for conditions that must be met before a conditional write is applied.
  */
sealed abstract class WriteCondition extends Serializable {
  def column : ColumnIdentifier
  def visibility : EntryVisibility
  def timestamp : Timestamp

  /**
    * Accumulo iterators to apply prior to evaluating the condition.
    */
  def iterators : immutable.Seq[IteratorConfiguration]

  /**
    * Constructs an Accumulo [[Condition]] object from this object.
    * @return a configured [[Condition]].
    */
  def toAccumuloCondition : Condition = {
    val condition = new Condition(column.family.toArray, column.qualifier.toArray)
    condition.setVisibility(visibility.parsed)
    timestamp match {
      case Timestamp.Specified(ts) => condition.setTimestamp(ts)
      case _ => condition
    }
    condition.setIterators(iterators.view.zipWithIndex.map { case (i, p) => i.toIteratorSetting(p)} : _*)
  }
}


object WriteCondition {

  /**
    * Write condition that verifies that a specified key is present and has a specified value.
    * @param column the column identifier to check.
    * @param value the value to check.
    * @param visibility the visibility to check.
    * @param timestamp the visibility to check; if not specified, the latest entry is checked.
    * @param iterators iterators to apply before evaluating the value.
    */
  final case class Present(
    column : ColumnIdentifier,
    value : EntryValue,
    visibility : EntryVisibility = EntryVisibility(),
    timestamp : Timestamp = Timestamp.Unspecified,
    iterators : immutable.Seq[IteratorConfiguration] = Nil
  ) extends WriteCondition {
    override def toAccumuloCondition : Condition = super.toAccumuloCondition.setValue(value.toArray)
  }

  /**
    * Write condition that verifies that a specified key is not present.
    * @param column the column identifier to check.
    * @param visibility the visibility to check.
    * @param timestamp the visibility to check; if not specified, the latest entry is checked.
    * @param iterators iterators to apply before evaluating the value.
    */
  final case class Absent(
    column : ColumnIdentifier,
    visibility : EntryVisibility = EntryVisibility(),
    timestamp : Timestamp = Timestamp.Unspecified,
    iterators : immutable.Seq[IteratorConfiguration] = Nil
  ) extends WriteCondition
}
