package io.github.reggert.cumulative.core.data

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.apache.accumulo.core.data.Key
import org.scalatest.{FunSuite, Matchers}


class EntryKeyTest extends FunSuite with Matchers {

  test("EntryKey constructor with defaults") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnIdentifier = ColumnIdentifier.fromString("b")
    val entryKey = new EntryKey(rowIdentifier, columnIdentifier)
    entryKey.getRowIdentifier should equal (rowIdentifier)
    entryKey.getColumnIdentifier should equal (columnIdentifier)
    entryKey.getVisibility should equal (EntryVisibility.DEFAULT)
    entryKey.getTimestamp should equal (Timestamp.UNSPECIFIED)
  }

  test("EntryKey.fromAccumuloKey") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnIdentifier = ColumnIdentifier.fromString("b")
    val visibility = EntryVisibility.fromString("abc")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val accumuloKey = new Key(
      rowIdentifier.toHadoopText,
      columnIdentifier.getFamily.toHadoopText,
      columnIdentifier.getQualifier.toHadoopText,
      visibility.toColumnVisibility,
      timestamp.longValue()
    )
    val entryKey = EntryKey.fromAccumuloKey(accumuloKey)
    entryKey.getRowIdentifier should equal (rowIdentifier)
    entryKey.getColumnIdentifier should equal (columnIdentifier)
    entryKey.getVisibility should equal (visibility)
    entryKey.getTimestamp should equal (timestamp)
    entryKey.toAccumuloKey should equal (accumuloKey)
  }

  test("EntryKey.equals") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnIdentifier = ColumnIdentifier.fromString("b")
    val visibility = EntryVisibility.fromString("abc")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnIdentifier, visibility, timestamp)
    entryKey should equal (entryKey)
    entryKey should equal (new EntryKey(rowIdentifier, columnIdentifier, visibility, timestamp))
    entryKey should not equal ("abc")
    entryKey should not equal (new EntryKey(RowIdentifier.fromString("x"), columnIdentifier, visibility, timestamp))
    entryKey should not equal (new EntryKey(rowIdentifier, ColumnIdentifier.EMPTY, visibility, timestamp))
    entryKey should not equal (new EntryKey(rowIdentifier, columnIdentifier, EntryVisibility.DEFAULT, timestamp))
    entryKey should not equal (new EntryKey(rowIdentifier, columnIdentifier, visibility, Timestamp.UNSPECIFIED))
  }

  test("EntryKey.compareTo") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val columnIdentifier = ColumnIdentifier.fromString("bef")
    val visibility = EntryVisibility.fromString("xyz")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnIdentifier, visibility, timestamp)
    entryKey should be <= new EntryKey(rowIdentifier, columnIdentifier, visibility, timestamp)
    entryKey should be > new EntryKey(RowIdentifier.fromString("ab"), columnIdentifier, visibility, timestamp)
    entryKey should be < new EntryKey(RowIdentifier.fromString("abcd"), columnIdentifier, visibility, timestamp)
    entryKey should be > new EntryKey(rowIdentifier, ColumnIdentifier.fromString("be"), visibility, timestamp)
    entryKey should be < new EntryKey(rowIdentifier, ColumnIdentifier.fromString("befg"), visibility, timestamp)
    entryKey should be > new EntryKey(rowIdentifier, columnIdentifier, EntryVisibility.fromString("xy"), timestamp)
    entryKey should be < new EntryKey(rowIdentifier, columnIdentifier, EntryVisibility.fromString("xyzw"), timestamp)
    entryKey should be > new EntryKey(rowIdentifier, columnIdentifier, visibility, new Timestamp(0L))
    entryKey should be < new EntryKey(rowIdentifier, columnIdentifier, visibility, Timestamp.UNSPECIFIED)
  }

  test("EntryKey.hashCode") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val columnIdentifier = ColumnIdentifier.fromString("bef")
    val visibility = EntryVisibility.fromString("xyz")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnIdentifier, visibility, timestamp)
    entryKey.hashCode() should equal (new EntryKey(rowIdentifier, columnIdentifier, visibility, timestamp).hashCode())
    entryKey.hashCode() should not equal (EntryKey.fromAccumuloKey(new Key).hashCode())
  }

  test("EntryKey.toString") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val columnIdentifier = ColumnIdentifier.fromString("bef")
    val visibility = EntryVisibility.fromString("xyz")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnIdentifier, visibility, timestamp)
    val string = entryKey.toString
    string should include (rowIdentifier.toString)
    string should include (columnIdentifier.toString)
    string should include (visibility.toString)
    string should include (timestamp.toString)
  }
}
