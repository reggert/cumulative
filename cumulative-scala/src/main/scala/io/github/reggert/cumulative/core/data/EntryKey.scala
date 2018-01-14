package io.github.reggert.cumulative.core.data

import org.apache.accumulo.core.data.Key


/**
  * Immutable representation of a key of an Accumulo entry.
  * @param row the row identifier.
  * @param column the column identifier.
  * @param visibility the visibility of the entry.
  * @param timestamp the timestmap of the entry, which may be left unspecified.
  */
final case class EntryKey(
  row : RowIdentifier,
  column : ColumnIdentifier = ColumnIdentifier(),
  visibility: EntryVisibility = EntryVisibility(),
  timestamp : Timestamp = Timestamp.Unspecified
) {
  /**
    * Converts this object to an Accumulo [[Key]] object.
    * @return a new [[Key]].
    */
  def toAccumuloKey : Key = timestamp match {
    case Timestamp.Specified(ts) =>
      new Key(row.toArray, column.family.toArray, column.qualifier.toArray, visibility.toArray, ts)
    case Timestamp.Unspecified =>
      new Key(row.toArray, column.family.toArray, column.qualifier.toArray, visibility.toArray)
  }
}


object EntryKey {
  /**
    * Constructs an EntryKey from an Accumulo [[Key]] object.
    * @param accumuloKey the [[Key]] object.
    * @return a new EntryKey.
    */
  def apply(accumuloKey : Key) : EntryKey = {
    val timestamp = accumuloKey.getTimestamp match {
      case Long.MaxValue => Timestamp.Unspecified
      case ts => Timestamp.Specified(ts)
    }
    EntryKey(
      RowIdentifier(accumuloKey.getRowData),
      ColumnIdentifier(
        ColumnFamily(accumuloKey.getColumnFamilyData),
        ColumnQualifier(accumuloKey.getColumnQualifierData)
      ),
      EntryVisibility(accumuloKey.getColumnVisibilityData),
      timestamp
    )
  }
}
