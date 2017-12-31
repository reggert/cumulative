package io.github.reggert.cumulative.core.data

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class EntryValueTest extends FunSuite with Matchers {

  test("EntryValue.fromByteSequence") {
    val byteSequence = ByteSequence.fromString("abc")
    val entryValue = EntryValue.fromByteSequence(byteSequence)
    entryValue.toByteSequence should equal (byteSequence)
  }

  test("EntryValue.fromByteArray") {
    val originalBytes = Seq[Byte](1, 2, 3)
    val byteArray = originalBytes.toArray
    val entryValue = EntryValue.fromByteArray(byteArray)
    byteArray(0) = -1
    byteArray(1) = -2
    byteArray(2) = -3
    entryValue.toByteArray.toSeq should equal (originalBytes)
  }

  test("EntryValue.fromHadoopText") {
    val hadoopText = new Text("abc")
    val entryValue = EntryValue.fromHadoopText(hadoopText)
    entryValue.toHadoopText should equal (hadoopText)
  }

  test("EntryValue.fromString") {
    val string = "abc"
    val entryValue = EntryValue.fromString(string)
    entryValue.toString should equal (string)
  }

  test("EntryValue.fromAccumuloValue") {
    val value = new Value("abc")
    val entryValue = EntryValue.fromAccumuloValue(value)
    entryValue.toString should equal ("abc")
  }

  test("EntryValue.compareTo") {
    EntryValue.fromString("abc") should be < EntryValue.fromString("abd")
    EntryValue.fromString("abc") should be > EntryValue.fromString("aaa")
    EntryValue.fromString("abc") should be > EntryValue.fromString("a")
    EntryValue.fromString("abc") should be < EntryValue.fromString("abcd")
    EntryValue.fromString("abc") should be <= EntryValue.fromString("abc")
  }

  test("EntryValue.equals") {
    val bs = EntryValue.fromString("abc")
    bs should equal (bs)
    bs should equal (EntryValue.fromString("abc"))
    bs should not equal ("abc")
    bs should not equal (EntryValue.fromString("abd"))
  }

  test("EntryValue.hashCode") {
    EntryValue.fromString("abc").hashCode() should equal (EntryValue.fromString("abc").hashCode())
    EntryValue.fromString("abc").hashCode() should not equal (EntryValue.fromString("abd").hashCode())
  }

}
