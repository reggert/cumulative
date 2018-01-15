package io.github.reggert.cumulative.core.mutate

import io.github.reggert.cumulative.core.data._
import org.apache.accumulo.core.data.{ConditionalMutation, Mutation}

import scala.collection.immutable


/**
  * Immutable representation of a set of changes to be applied atomically to a row.
  *
  * @param row the identifier of the row to which the changes apply.
  * @param changes the per-entry changes to be applied.
  */
final case class RowMutation(
  row : RowIdentifier,
  changes : immutable.Seq[EntryChange]
) {
  /**
    * Creates an Accumulo [[Mutation]] object from this object.
    * @return a new [[Mutation]].
    */
  def toAccumuloMutation : Mutation = {
    val mutation = new Mutation(row.toArray)
    changes.foreach(_(mutation))
    mutation
  }
}


/**
  * Immutable representation of a set of changes to be applied atomically to a row if certain conditions
  * are met.
  *
  * @param unconditional the unconditional mutation to apply.
  * @param conditions the conditions to apply to the mutation.
  */
final case class ConditionalRowMutation(
  unconditional : RowMutation,
  conditions : immutable.Seq[WriteCondition]
) {
  /**
    * Creates an Accumulo [[ConditionalMutation]] object from this object.
    * @return a new [[ConditionalMutation]].
    */
  def toAccumuloMutation : ConditionalMutation = {
    val mutation = new ConditionalMutation(unconditional.row.toArray, conditions.map(_.toAccumuloCondition) : _*)
    unconditional.changes.foreach(_(mutation))
    mutation
  }
}


object RowMutation {
  /**
    * Constructs a `RowMutation` that writes a new row, replacing any matching existing entries, but not
    * deleting any existing unmatched entries.
    *
    * @param row the row to write.
    * @return a new `RowMutation` that contains all the entries from the row as instances of
    *         [[EntryChange.Put]].
    */
  def createRow(row : Row) : RowMutation = {
    val changes = for {
      (cf, fm) <- row.columns
      (cq, ce) <- fm
    } yield EntryChange.Put(ColumnIdentifier(cf, cq), ce.value, ce.visibility, ce.timestamp)
    RowMutation(row.id, changes.toList)
  }

  /**
    * Constructs a `RowMutation` that deletes a row, adding delete markings for matching existing entries,
    * but not deleting any existing unmatched entries.
    *
    * @param row the row to delete.
    * @return a new `RowMutation` that contains all the entries from the row as instances of
    *         [[EntryChange.Delete]].
    */
  def deleteRow(row : Row) : RowMutation = {
    val changes = for {
      (cf, fm) <- row.columns
      (cq, ce) <- fm
    } yield EntryChange.Delete(ColumnIdentifier(cf, cq), ce.visibility)
    RowMutation(row.id, changes.toList)
  }

  /**
    * Constructs a `RowMutation` that applies changes necessary to change the state of an existing row
    * to a specified new state.
    *
    * @param from the existing row state.
    * @param to the desired row state.
    * @return a new `RowMutation` that contains [[EntryChange.Put]] instances for any entries to be added
    *         or modified, and [[EntryChange.Delete]] instances for any entries to be removed. Entries for
    *         which the only difference is the timestamp will not be modified.
    * @throws IllegalArgumentException if the rowids do not match.
    */
  def delta(from : Row, to : Row) : RowMutation = {
    require(from.id == to.id, s"Cannot compute delete for different row ids (${from.id} != ${to.id}")
    val fromMap = for {
      (cf, fm) <- from.columns
      (cq, ce) <- fm
    } yield (ColumnIdentifier(cf, cq), ce.visibility) -> (ce.value, ce.timestamp)
    val toMap = for {
      (cf, fm) <- to.columns
      (cq, ce) <- fm
    } yield (ColumnIdentifier(cf, cq), ce.visibility) -> (ce.value, ce.timestamp)
    val deletions = fromMap.keySet.view.filterNot(toMap.keySet).map {
      case (c, vis) => EntryChange.Delete(c, vis)
    }
    val existingValues = fromMap.map { case ((c, vis), (v, _)) => (c, vis, v) }.toSet
    val puts = toMap.view.filter {
      case ((c, vis), (v, ts)) => !existingValues((c, vis, v))
    }.map {
      case ((c, vis), (v, ts)) => EntryChange.Put(c, v, vis, ts)
    }
    RowMutation(to.id, (deletions ++ puts).toList)
  }

  /**
    * Constructs a `RowMutation` to write a single entry.
    * @param entry the entry to write.
    * @return a `RowMutation` containing a single `EntryChange`
    */
  def putEntry(entry : Entry) : RowMutation = RowMutation(
    entry.key.row,
    List(EntryChange.Put(entry.key.column, entry.value, entry.key.visibility, entry.key.timestamp))
  )

  /**
    * Constructs a `RowMutation` to delete a single entry.
    * @param entryKey the key of the entry to delete.
    * @return a `RowMutation` containing a single `EntryChange`
    */
  def deleteEntry(entryKey : EntryKey) : RowMutation = RowMutation(
    entryKey.row,
    List(EntryChange.Delete(entryKey.column, entryKey.visibility))
  )
}


sealed abstract class EntryChange extends Serializable {
  def column : ColumnIdentifier
  def visibility: EntryVisibility
  def timestamp: Timestamp

  /**
    * Applies the change to an Accumulo [[Mutation]] object.
    *
    * @param mutation the object to which to apply the change.
    */
  def apply(mutation : Mutation) : Unit
}


object EntryChange {
  final case class Put(
    column : ColumnIdentifier,
    value : EntryValue,
    visibility : EntryVisibility = EntryVisibility(),
    timestamp: Timestamp = Timestamp.Unspecified
  ) extends EntryChange {
    override def apply(mutation: Mutation): Unit = timestamp match {
      case Timestamp.Unspecified =>
        mutation.put(column.family.toArray, column.qualifier.toArray, visibility.parsed, value.toArray)
      case Timestamp.Specified(ts) =>
        mutation.put(column.family.toArray, column.qualifier.toArray, visibility.parsed, ts, value.toArray)
    }
  }

  final case class Delete(
    column : ColumnIdentifier,
    visibility : EntryVisibility = EntryVisibility(),
    timestamp: Timestamp = Timestamp.Unspecified
  ) extends EntryChange {
    override def apply(mutation: Mutation): Unit = timestamp match {
      case Timestamp.Unspecified =>
        mutation.putDelete(column.family.toArray, column.qualifier.toArray, visibility.parsed)
      case Timestamp.Specified(ts) =>
        mutation.putDelete(column.family.toArray, column.qualifier.toArray, visibility.parsed, ts)
    }
  }
}
