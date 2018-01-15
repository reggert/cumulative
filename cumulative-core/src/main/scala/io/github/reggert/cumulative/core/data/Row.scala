package io.github.reggert.cumulative.core.data

import java.io.IOException
import java.util

import io.github.reggert.cumulative.core.data.Row.{ColumnEntry, ColumnMap}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.user.WholeRowIterator

import scala.collection.JavaConverters._
import scala.collection.immutable


/**
  * Immutable representation of an Accumulo row.
  *
  * Note that Accumulo technically allows rows to have multiple copies of the same column with different
  * visibilities and timestamps, but this class disallows such duplication, in order to better reflect the
  * behavior that most users would expect.
  *
  * @param id the row identifier.
  * @param columns sorted, bi-level map of columns to column entries.
  */
final case class Row(id : RowIdentifier, columns : ColumnMap) {
  /**
    * Converts this row into a sequence of entries returned in sorted order.
    */
  def entries: Iterable[Entry] = for {
    (family, familyMap) <- columns.view
    (qualifier, ColumnEntry(value, visibility, timestamp)) <- familyMap.view
  } yield Entry(EntryKey(id, ColumnIdentifier(family, qualifier), visibility, timestamp), value)

  /**
    * This implementation returns the Accumulo shell representation of each entry, separated by newlines.
    */
  override def toString : String = entries.mkString("\n")

  /**
    * Merges two rows with the same id together.
    * @param that a row with the same identifier as this row; must not contain duplicate columns.
    * @return a new row containing the combined entry maps.
    * @throws IllegalArgumentException if the rowids do not match, or if they contain duplicate columns.
    */
  def + (that : Row) : Row = {
    require(this.id == that.id, s"Cannot combine rows with different identifiers (${this.id} != ${that.id})")
    copy(
      columns = (this.columns /: that.columns) {(fm1, fe) =>
        val (f, qm2) = fe
        val qm1 = fm1.getOrElse(f, immutable.SortedMap.empty[ColumnQualifier, ColumnEntry])
        fm1.updated(
          f,
          (qm1 /: qm2) {(qm, qe) =>
            val (q, e) = qe
            require(!qm.contains(q), s"Duplicate entry found for column ($f, $q)")
            qm.updated(q, e)
          }
        )
      }
    )
  }

  /**
    * Adds the specified entry to this row, replacing any previous value for its column.
    *
    * @param entry the entry to add; the rowid must match this row's id.
    * @return a new Row.
    */
  def :+ (entry : Entry) : Row = {
    require(
      this.id == entry.key.row,
      s"Cannot add entry for another row (${this.id.bytes} != ${entry.key.row.bytes}"
    )
    copy(
      columns = columns.updated(
        entry.key.column.family,
        columns.getOrElse(
          entry.key.column.family,
          immutable.SortedMap.empty[ColumnQualifier, ColumnEntry]
        ).updated(
          entry.key.column.qualifier,
          ColumnEntry(entry.value, entry.key.visibility, entry.key.timestamp)
        )
      )
    )
  }

  /**
    * Adds the specified entries to this row, overwriting any matching columns in this row.
    *
    * @param entries the entries to add; the rowids must match this row's id.
    * @return a new Row.
    */
  def :++ (entries : Traversable[Entry]) : Row = (this /: entries) {(r, e) => r :+ e}

  /**
    * Encodes the row as a single [[Entry]] using [[WholeRowIterator]].
    *
    * @return an entry whose value contains the encoded entries for this row.
    */
  def encoded : Entry = {
    val kvPairs = entries.map(_.toAccumuloEntry).map(e => e.getKey -> e.getValue).toMap
    val keys = new util.ArrayList[Key](kvPairs.keys.asJavaCollection)
    val values = new util.ArrayList[Value](kvPairs.values.asJavaCollection)
    val encodedValue = EntryValue(WholeRowIterator.encodeRow(keys, values))
    Entry(EntryKey(id), encodedValue)
  }
}


object Row {
  /**
    * Constructs a `Row` from a collection of entries.
    *
    * @param entries a non-empty collection of entries with the same rowid.
    * @return a new `Row`.
    * @throws IllegalArgumentException if the collection is emtpy or the rowids are not the same.
    */
  def apply(entries : Traversable[Entry]) : Row = {
    require(entries.nonEmpty, "Cannot construct row from empty collection")
    val firstEntry = entries.head
    val initial = Row(
      firstEntry.key.row,
      immutable.TreeMap(
        firstEntry.key.column.family -> immutable.TreeMap(
          firstEntry.key.column.qualifier -> ColumnEntry(
            firstEntry.value,
            firstEntry.key.visibility,
            firstEntry.key.timestamp
          )
        )
      )
    )
    (initial /: entries.tail)(_ :+ _)
  }

  /**
    * Decodes an entry that has been encoded using [[WholeRowIterator]].
    *
    * @param entry a row that has been encoded into a single entry.
    * @return the decoded row.
    * @throws IOException if an error is encountered while decoding the row.
    */
  def decode(entry : Entry) : Row = Row(
    WholeRowIterator.decodeRow(
      entry.key.toAccumuloKey,
      entry.value.toAccumuloValue
    ).entrySet().asScala.view.map(Entry.apply)
  )

  /**
    * Triple of the value, visibility, and timestamp associated with a column within a row.
    * @param value value of the entry.
    * @param visibility visiblity of the entry.
    * @param timestamp timestamp of the entry.
    */
  final case class ColumnEntry(
    value : EntryValue,
    visibility: EntryVisibility = EntryVisibility(),
    timestamp: Timestamp = Timestamp.Unspecified
  )
  type ColumnMap = immutable.SortedMap[ColumnFamily, immutable.SortedMap[ColumnQualifier, ColumnEntry]]
}

