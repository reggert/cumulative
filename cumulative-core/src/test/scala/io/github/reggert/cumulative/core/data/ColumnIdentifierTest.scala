package io.github.reggert.cumulative.core.data

import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class ColumnIdentifierTest extends FunSuite with Matchers {

  test("ColumnIdentifier.fromAccumuloKey") {
    val columnFamily = ColumnFamily.fromString("a")
    val columnQualifier = ColumnQualifier.fromString("b")
    val accumuloKey = new Key(new Text("xyz"), columnFamily.toHadoopText, columnQualifier.toHadoopText)
    val columnIdentifier = ColumnIdentifier.fromAccumuloKey(accumuloKey)
    columnIdentifier should equal (new ColumnIdentifier(columnFamily, columnQualifier))
  }

  test("ColumnIdentifier.equals") {
    val columnFamily = ColumnFamily.fromString("a")
    val columnQualifier = ColumnQualifier.fromString("b")
    val columnIdentifier = new ColumnIdentifier(columnFamily, columnQualifier)
    columnIdentifier should equal (columnIdentifier)
    columnIdentifier should equal (new ColumnIdentifier(columnFamily, columnQualifier))
    columnIdentifier should not equal ("abc")
    columnIdentifier should not equal (new ColumnIdentifier(ColumnFamily.EMPTY, columnQualifier))
    columnIdentifier should not equal (new ColumnIdentifier(columnFamily, ColumnQualifier.EMPTY))
  }

  test("ColumnIdentifier.hashCode") {
    val columnFamily = ColumnFamily.fromString("a")
    val columnQualifier = ColumnQualifier.fromString("b")
    val columnIdentifier = new ColumnIdentifier(columnFamily, columnQualifier)
    columnIdentifier.hashCode() should equal (new ColumnIdentifier(columnFamily, columnQualifier).hashCode())
    columnIdentifier.hashCode() should not equal (ColumnIdentifier.EMPTY.hashCode())
  }

  test("ColumnIdentifier.toString") {
    val columnFamily = ColumnFamily.fromString("a")
    val columnQualifier = ColumnQualifier.fromString("b")
    val columnIdentifier = new ColumnIdentifier(columnFamily, columnQualifier)
    val string = columnIdentifier.toString
    string should include (columnFamily.toString)
    string should include (columnQualifier.toString)
  }
}
