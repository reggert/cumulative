package io.github.reggert.cumulative.core.data

import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class RowIdentifierTest extends FunSuite with Matchers {

  test("RowIdentifier.fromByteSequence") {
    val byteSequence = ByteSequence.fromString("abc")
    val rowIdentifier = RowIdentifier.fromByteSequence(byteSequence)
    rowIdentifier.toByteSequence should equal (byteSequence)
  }

  test("RowIdentifier.fromByteArray") {
    val originalBytes = Seq[Byte](1, 2, 3)
    val byteArray = originalBytes.toArray
    val rowIdentifier = RowIdentifier.fromByteArray(byteArray)
    byteArray(0) = -1
    byteArray(1) = -2
    byteArray(2) = -3
    rowIdentifier.toByteArray.toSeq should equal (originalBytes)
  }

  test("RowIdentifier.fromHadoopText") {
    val hadoopText = new Text("abc")
    val rowIdentifier = RowIdentifier.fromHadoopText(hadoopText)
    rowIdentifier.toHadoopText should equal (hadoopText)
  }

  test("RowIdentifier.fromString") {
    val string = "abc"
    val rowIdentifier = RowIdentifier.fromString(string)
    rowIdentifier.toString should equal (string)
  }

  test("RowIdentifier.fromAccumuloKey") {
    val key = new Key(new Text("abc"))
    val rowIdentifier = RowIdentifier.fromAccumuloKey(key)
    rowIdentifier.toString should equal ("abc")
  }

  test("RowIdentifier.compareTo") {
    RowIdentifier.fromString("abc") should be < RowIdentifier.fromString("abd")
    RowIdentifier.fromString("abc") should be > RowIdentifier.fromString("aaa")
    RowIdentifier.fromString("abc") should be > RowIdentifier.fromString("a")
    RowIdentifier.fromString("abc") should be < RowIdentifier.fromString("abcd")
    RowIdentifier.fromString("abc") should be <= RowIdentifier.fromString("abc")
  }

  test("RowIdentifier.equals") {
    val bs = RowIdentifier.fromString("abc")
    bs should equal (bs)
    bs should equal (RowIdentifier.fromString("abc"))
    bs should not equal ("abc")
    bs should not equal (RowIdentifier.fromString("abd"))
  }

  test("RowIdentifier.hashCode") {
    RowIdentifier.fromString("abc").hashCode() should equal (RowIdentifier.fromString("abc").hashCode())
    RowIdentifier.fromString("abc").hashCode() should not equal (RowIdentifier.fromString("abd").hashCode())
  }

}
