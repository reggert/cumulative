package io.github.reggert.cumulative.core.data

import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class ColumnQualifierTest extends FunSuite with Matchers {

  test("ColumnQualifier.fromByteSequence") {
    val byteSequence = ByteSequence.fromString("abc")
    val columnQualifier = ColumnQualifier.fromByteSequence(byteSequence)
    columnQualifier.toByteSequence should equal (byteSequence)
  }

  test("ColumnQualifier.fromByteArray") {
    val originalBytes = Seq[Byte](1, 2, 3)
    val byteArray = originalBytes.toArray
    val columnQualifier = ColumnQualifier.fromByteArray(byteArray)
    byteArray(0) = -1
    byteArray(1) = -2
    byteArray(2) = -3
    columnQualifier.toByteArray.toSeq should equal (originalBytes)
  }

  test("ColumnQualifier.fromHadoopText") {
    val hadoopText = new Text("abc")
    val columnQualifier = ColumnQualifier.fromHadoopText(hadoopText)
    columnQualifier.toHadoopText should equal (hadoopText)
  }

  test("ColumnQualifier.fromString") {
    val string = "abc"
    val columnQualifier = ColumnQualifier.fromString(string)
    columnQualifier.toString should equal (string)
  }

  test("ColumnQualifier.fromAccumuloKey") {
    val key = new Key(new Text("abc"), new Text("def"), new Text("hij"))
    val columnQualifier = ColumnQualifier.fromAccumuloKey(key)
    columnQualifier.toString should equal ("hij")
  }

  test("ColumnQualifier.compareTo") {
    ColumnQualifier.fromString("abc") should be < ColumnQualifier.fromString("abd")
    ColumnQualifier.fromString("abc") should be > ColumnQualifier.fromString("aaa")
    ColumnQualifier.fromString("abc") should be > ColumnQualifier.fromString("a")
    ColumnQualifier.fromString("abc") should be < ColumnQualifier.fromString("abcd")
    ColumnQualifier.fromString("abc") should be <= ColumnQualifier.fromString("abc")
  }

  test("ColumnQualifier.equals") {
    val bs = ColumnQualifier.fromString("abc")
    bs should equal (bs)
    bs should equal (ColumnQualifier.fromString("abc"))
    bs should not equal ("abc")
    bs should not equal (ColumnQualifier.fromString("abd"))
  }

  test("ColumnQualifier.hashCode") {
    ColumnQualifier.fromString("abc").hashCode() should equal (ColumnQualifier.fromString("abc").hashCode())
    ColumnQualifier.fromString("abc").hashCode() should not equal (ColumnQualifier.fromString("abd").hashCode())
  }

}
