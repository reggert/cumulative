package io.github.reggert.cumulative.core.data

import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class ColumnFamilyTest extends FunSuite with Matchers {

  test("ColumnFamily.fromByteSequence") {
    val byteSequence = ByteSequence.fromString("abc")
    val columnFamily = ColumnFamily.fromByteSequence(byteSequence)
    columnFamily.toByteSequence should equal (byteSequence)
  }

  test("ColumnFamily.fromByteArray") {
    val originalBytes = Seq[Byte](1, 2, 3)
    val byteArray = originalBytes.toArray
    val columnFamily = ColumnFamily.fromByteArray(byteArray)
    byteArray(0) = -1
    byteArray(1) = -2
    byteArray(2) = -3
    columnFamily.toByteArray.toSeq should equal (originalBytes)
  }

  test("ColumnFamily.fromHadoopText") {
    val hadoopText = new Text("abc")
    val columnFamily = ColumnFamily.fromHadoopText(hadoopText)
    columnFamily.toHadoopText should equal (hadoopText)
  }

  test("ColumnFamily.fromString") {
    val string = "abc"
    val columnFamily = ColumnFamily.fromString(string)
    columnFamily.toString should equal (string)
  }

  test("ColumnFamily.fromAccumuloKey") {
    val key = new Key(new Text("abc"), new Text("def"))
    val columnFamily = ColumnFamily.fromAccumuloKey(key)
    columnFamily.toString should equal ("def")
  }

  test("ColumnFamily.compareTo") {
    ColumnFamily.fromString("abc") should be < ColumnFamily.fromString("abd")
    ColumnFamily.fromString("abc") should be > ColumnFamily.fromString("aaa")
    ColumnFamily.fromString("abc") should be > ColumnFamily.fromString("a")
    ColumnFamily.fromString("abc") should be < ColumnFamily.fromString("abcd")
    ColumnFamily.fromString("abc") should be <= ColumnFamily.fromString("abc")
  }

  test("ColumnFamily.equals") {
    val bs = ColumnFamily.fromString("abc")
    bs should equal (bs)
    bs should equal (ColumnFamily.fromString("abc"))
    bs should not equal ("abc")
    bs should not equal (ColumnFamily.fromString("abd"))
  }

  test("ColumnFamily.hashCode") {
    ColumnFamily.fromString("abc").hashCode() should equal (ColumnFamily.fromString("abc").hashCode())
    ColumnFamily.fromString("abc").hashCode() should not equal (ColumnFamily.fromString("abd").hashCode())
  }

}
