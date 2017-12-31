package io.github.reggert.cumulative.core.data

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.apache.accumulo.core.data.Key
import org.scalatest.{FunSuite, Matchers}

class EntryKeyTest extends FunSuite with Matchers {

  test("EntryKey constructor with defaults") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnFamily = ColumnFamily.fromString("b")
    val columnQualifier = ColumnQualifier.fromString("c")
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier)
    entryKey.getRowIdentifier should equal (rowIdentifier)
    entryKey.getColumnFamily should equal (columnFamily)
    entryKey.getColumnQualifier should equal (columnQualifier)
    entryKey.getVisibility should equal (EntryVisibility.DEFAULT)
    entryKey.getTimestamp should equal (Timestamp.UNSPECIFIED)
  }

  test("EntryKey.fromAccumuloKey") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnFamily = ColumnFamily.fromString("b")
    val columnQualifier = ColumnQualifier.fromString("c")
    val visibility = EntryVisibility.fromString("abc")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val accumuloKey = new Key(
      rowIdentifier.toHadoopText,
      columnFamily.toHadoopText,
      columnQualifier.toHadoopText,
      visibility.toColumnVisibility,
      timestamp.longValue()
    )
    val entryKey = EntryKey.fromAccumuloKey(accumuloKey)
    entryKey.getRowIdentifier should equal (rowIdentifier)
    entryKey.getColumnFamily should equal (columnFamily)
    entryKey.getColumnQualifier should equal (columnQualifier)
    entryKey.getVisibility should equal (visibility)
    entryKey.getTimestamp should equal (timestamp)
    entryKey.toAccumuloKey should equal (accumuloKey)
  }

  test("EntryKey.equals") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnFamily = ColumnFamily.fromString("b")
    val columnQualifier = ColumnQualifier.fromString("c")
    val visibility = EntryVisibility.fromString("abc")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp)
    entryKey should equal (entryKey)
    entryKey should equal (new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp))
    entryKey should not equal ("abc")
    entryKey should not equal (new EntryKey(RowIdentifier.fromString("x"), columnFamily, columnQualifier, visibility, timestamp))
    entryKey should not equal (new EntryKey(rowIdentifier, ColumnFamily.EMPTY, columnQualifier, visibility, timestamp))
    entryKey should not equal (new EntryKey(rowIdentifier, columnFamily, ColumnQualifier.EMPTY, visibility, timestamp))
    entryKey should not equal (new EntryKey(rowIdentifier, columnFamily, columnQualifier, EntryVisibility.DEFAULT, timestamp))
    entryKey should not equal (new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, Timestamp.UNSPECIFIED))
  }

  test("EntryKey.compareTo") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val columnFamily = ColumnFamily.fromString("bef")
    val columnQualifier = ColumnQualifier.fromString("cde")
    val visibility = EntryVisibility.fromString("xyz")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp)
    entryKey should be <= new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp)
    entryKey should be > new EntryKey(RowIdentifier.fromString("ab"), columnFamily, columnQualifier, visibility, timestamp)
    entryKey should be < new EntryKey(RowIdentifier.fromString("abcd"), columnFamily, columnQualifier, visibility, timestamp)
    entryKey should be > new EntryKey(rowIdentifier, ColumnFamily.fromString("be"), columnQualifier, visibility, timestamp)
    entryKey should be < new EntryKey(rowIdentifier, ColumnFamily.fromString("befg"), columnQualifier, visibility, timestamp)
    entryKey should be > new EntryKey(rowIdentifier, columnFamily, ColumnQualifier.fromString("cd"), visibility, timestamp)
    entryKey should be < new EntryKey(rowIdentifier, columnFamily, ColumnQualifier.fromString("cdef"), visibility, timestamp)
    entryKey should be > new EntryKey(rowIdentifier, columnFamily, columnQualifier, EntryVisibility.fromString("xy"), timestamp)
    entryKey should be < new EntryKey(rowIdentifier, columnFamily, columnQualifier, EntryVisibility.fromString("xyzw"), timestamp)
    entryKey should be > new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, new Timestamp(0L))
    entryKey should be < new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, Timestamp.UNSPECIFIED)
  }

  test("EntryKey.hashCode") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val columnFamily = ColumnFamily.fromString("bef")
    val columnQualifier = ColumnQualifier.fromString("cde")
    val visibility = EntryVisibility.fromString("xyz")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp)
    entryKey.hashCode() should equal (new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp).hashCode())
    entryKey.hashCode() should not equal (EntryKey.fromAccumuloKey(new Key).hashCode())
  }

  test("EntryKey.toString") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val columnFamily = ColumnFamily.fromString("bef")
    val columnQualifier = ColumnQualifier.fromString("cde")
    val visibility = EntryVisibility.fromString("xyz")
    val timestamp = Timestamp.fromInstant(Instant.now().truncatedTo(ChronoUnit.MILLIS))
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier, visibility, timestamp)
    val string = entryKey.toString
    string should include (rowIdentifier.toString)
    string should include (columnFamily.toString)
    string should include (columnQualifier.toString)
    string should include (visibility.toString)
    string should include (timestamp.toString)
  }
}
