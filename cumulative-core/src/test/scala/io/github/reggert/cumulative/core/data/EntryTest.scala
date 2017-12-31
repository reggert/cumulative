package io.github.reggert.cumulative.core.data

import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._


class EntryTest extends FunSuite with Matchers {

  test("Entry.fromAccumuloEntry") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnFamily = ColumnFamily.fromString("b")
    val columnQualifier = ColumnQualifier.fromString("c")
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier)
    val entryValue = EntryValue.fromString("value")
    val accumuloEntries = Map(entryKey.toAccumuloKey -> entryValue.toAccumuloValue).asJava
    val accumuloEntry = accumuloEntries.entrySet().iterator().next()
    val entry = Entry.fromAccumuloEntry(accumuloEntry)
    entry should equal (new Entry(entryKey, entryValue))
    entry.getKey should equal (entryKey)
    entry.getValue should equal (entryValue)
  }

  test("Entry.equals") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnFamily = ColumnFamily.fromString("b")
    val columnQualifier = ColumnQualifier.fromString("c")
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier)
    val entryValue = EntryValue.fromString("value")
    val entry = new Entry(entryKey, entryValue)
    entry should equal (entry)
    entry should equal (new Entry(entryKey, entryValue))
    entry should not equal ("abc")
    entry should not equal (new Entry(new EntryKey(rowIdentifier, ColumnFamily.EMPTY, ColumnQualifier.EMPTY), entryValue))
    entry should not equal (new Entry(entryKey, EntryValue.EMPTY))
  }

  test("Entry.hashCode") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnFamily = ColumnFamily.fromString("b")
    val columnQualifier = ColumnQualifier.fromString("c")
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier)
    val entryValue = EntryValue.fromString("value")
    val entry = new Entry(entryKey, entryValue)
    entry.hashCode() should equal (new Entry(entryKey, entryValue).hashCode())
    entry.hashCode() should not equal (new Entry(entryKey, EntryValue.EMPTY).hashCode())
  }

  test("Entry.toString") {
    val rowIdentifier = RowIdentifier.fromString("a")
    val columnFamily = ColumnFamily.fromString("b")
    val columnQualifier = ColumnQualifier.fromString("c")
    val entryKey = new EntryKey(rowIdentifier, columnFamily, columnQualifier)
    val entryValue = EntryValue.fromString("value")
    val entry = new Entry(entryKey, entryValue)
    val string = entry.toString
    string should include (entryKey.toString)
    string should include (entryValue.toString)
  }
}
