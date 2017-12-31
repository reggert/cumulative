package io.github.reggert.cumulative.core.data

import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._


class RowTest extends FunSuite with Matchers {
  test("Row.fromEntries handles the simple case") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val column1 = ColumnIdentifier.fromString("column1")
    val column2 = ColumnIdentifier.fromString("column2")
    val columnValues = Map(
      column1 -> EntryValue.fromString("value1"),
      column2 -> EntryValue.fromString("value2")
    )
    val entries = columnValues.map {case (c, v) => new Entry(new EntryKey(rowIdentifier, c), v)}.toSeq
    val row = Row.fromEntries(entries.asJava)
    row.getIdentifier should equal (rowIdentifier)
    row.getEntries.get(column1).getValue should equal (columnValues(column1))
    row.getEntries.get(column2).getValue should equal (columnValues(column2))
  }

  test("Row.fromEntries rejects an empty list") {
    assertThrows[IllegalStateException] {
      Row.fromEntries(Nil.asJava)
    }
  }

  test("Row.fromEntries rejects mixed rows") {
    val entries = Seq(
      new Entry(new EntryKey(RowIdentifier.fromString("abc"), ColumnIdentifier.EMPTY), EntryValue.EMPTY),
      new Entry(new EntryKey(RowIdentifier.fromString("xyz"), ColumnIdentifier.EMPTY), EntryValue.EMPTY)
    )
    assertThrows[IllegalArgumentException] {
      Row.fromEntries(entries.asJava)
    }
  }

  test("Row.fromEntries handles duplicates") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val column = ColumnIdentifier.fromString("column1")
    val oldValue1 = EntryValue.fromString("oldvalue1")
    val oldValue2 = EntryValue.fromString("oldvalue2")
    val visValue = EntryValue.fromString("visvalue")
    val newValue = EntryValue.fromString("newvalue")
    val entries = Seq(
      new Entry(new EntryKey(rowIdentifier, column, EntryVisibility.DEFAULT, new Timestamp(100L)), oldValue1),
      new Entry(new EntryKey(rowIdentifier, column, EntryVisibility.DEFAULT, Timestamp.UNSPECIFIED), newValue),
      new Entry(new EntryKey(rowIdentifier, column, EntryVisibility.fromString("x"), Timestamp.UNSPECIFIED), visValue),
      new Entry(new EntryKey(rowIdentifier, column, EntryVisibility.DEFAULT, new Timestamp(200L)), oldValue2)
    )
    val row = Row.fromEntries(entries.asJava)
    row.getIdentifier should equal (rowIdentifier)
    row.getEntries should have size (1)
    row.getEntries.get(column).getValue should equal (newValue)
  }

  test("Row.iterator handles an empty source iterator") {
    val rowIterator = Row.iterator(Nil.iterator.asJava)
    rowIterator.asScala shouldBe empty
  }

  test("Row.iterator should throw NoSuchElementException when overconsumed") {
    val rowIterator = Row.iterator(Nil.iterator.asJava)
    assertThrows[NoSuchElementException] {
      rowIterator.next()
    }
  }

  test("Row.iterator should handle multiple rows") {
    val rowIdentifier1 = RowIdentifier.fromString("abc")
    val rowIdentifier2 = RowIdentifier.fromString("xyz")
    val column1 = ColumnIdentifier.fromString("column1")
    val column2 = ColumnIdentifier.fromString("column2")
    val columnValues1 = Map(
      column1 -> EntryValue.fromString("value1a"),
      column2 -> EntryValue.fromString("value2a")
    )
    val columnValues2 = Map(
      column1 -> EntryValue.fromString("value1b"),
      column2 -> EntryValue.fromString("value2b")
    )
    val entries1 = columnValues1.map {case (c, v) => new Entry(new EntryKey(rowIdentifier1, c), v)}.toSeq
    val entries2 = columnValues2.map {case (c, v) => new Entry(new EntryKey(rowIdentifier2, c), v)}.toSeq
    val rows = Row.iterator((entries1 ++ entries2).asJava.iterator()).asScala.toSeq
    rows should have size (2)
    rows(0).getIdentifier should equal (rowIdentifier1)
    rows(0).getEntries.asScala should have size (2)
    rows(0).getEntries.get(column1).getValue should equal (columnValues1(column1))
    rows(0).getEntries.get(column2).getValue should equal (columnValues1(column2))
    rows(1).getIdentifier should equal (rowIdentifier2)
    rows(1).getEntries.asScala should have size (2)
    rows(1).getEntries.get(column1).getValue should equal (columnValues2(column1))
    rows(1).getEntries.get(column2).getValue should equal (columnValues2(column2))
  }

  test("Row.toBuilder.build should return an equivalent row") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val column1 = ColumnIdentifier.fromString("column1")
    val column2 = ColumnIdentifier.fromString("column2")
    val columnValues = Map(
      column1 -> EntryValue.fromString("value1"),
      column2 -> EntryValue.fromString("value2")
    )
    val entries = columnValues.map {case (c, v) => new Entry(new EntryKey(rowIdentifier, c), v)}.toSeq
    val row = Row.fromEntries(entries.asJava)
    row.toBuilder.build() should equal (row)
  }

  test("Row.toString") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val column1 = ColumnIdentifier.fromString("column1")
    val column2 = ColumnIdentifier.fromString("column2")
    val columnValues = Map(
      column1 -> EntryValue.fromString("value1"),
      column2 -> EntryValue.fromString("value2")
    )
    val entries = columnValues.map {case (c, v) => new Entry(new EntryKey(rowIdentifier, c), v)}.toSeq
    val row = Row.fromEntries(entries.asJava)
    val string = row.toString
    string should include (rowIdentifier.toString)
    string should include (column1.toString)
    string should include (column2.toString)
    string should include (columnValues(column1).toString)
    string should include (columnValues(column2).toString)
  }

  test("Row.equals") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val column1 = ColumnIdentifier.fromString("column1")
    val column2 = ColumnIdentifier.fromString("column2")
    val columnValues = Map(
      column1 -> EntryValue.fromString("value1"),
      column2 -> EntryValue.fromString("value2")
    )
    val entries = columnValues.map {case (c, v) => new Entry(new EntryKey(rowIdentifier, c), v)}.toSeq
    val row = Row.fromEntries(entries.asJava)
    row should equal (row)
    row should equal (Row.fromEntries(entries.asJava))
    row should not equal ("abc")
    row should not equal (Row.fromEntries(entries.take(1).asJava))
  }

  test("Row.hashCode") {
    val rowIdentifier = RowIdentifier.fromString("abc")
    val column1 = ColumnIdentifier.fromString("column1")
    val column2 = ColumnIdentifier.fromString("column2")
    val columnValues = Map(
      column1 -> EntryValue.fromString("value1"),
      column2 -> EntryValue.fromString("value2")
    )
    val entries = columnValues.map {case (c, v) => new Entry(new EntryKey(rowIdentifier, c), v)}.toSeq
    val row = Row.fromEntries(entries.asJava)
    row.hashCode() should equal (Row.fromEntries(entries.asJava).hashCode())
    row.hashCode() should not equal (Row.fromEntries(entries.take(1).asJava).hashCode())
  }
}
