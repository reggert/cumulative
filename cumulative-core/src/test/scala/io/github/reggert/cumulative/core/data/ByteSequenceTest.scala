package io.github.reggert.cumulative.core.data

import java.nio.charset.Charset

import org.apache.accumulo.core.data.Value
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class ByteSequenceTest extends FunSuite with Matchers {

  test("ByteSequence.fromByteArray") {
    val byteArray = Array[Byte](1, 2, 3)
    val byteSequence = ByteSequence.fromByteArray(byteArray)
    byteArray(0) = -1
    byteArray(1) = -2
    byteArray(2) = -3
    byteSequence.get(0) should equal (1)
    byteSequence.get(1) should equal (2)
    byteSequence.get(2) should equal (3)
  }

  test("ByteSequence.fromHadoopText") {
    val hadoopText = new Text("abc")
    val byteSequence = ByteSequence.fromHadoopText(hadoopText)
    byteSequence.toHadoopText should equal (hadoopText)
  }

  test("ByteSequence.fromString") {
    val string = "abc"
    val byteSequence = ByteSequence.fromString(string)
    byteSequence.toString should equal (string)
  }

  test("ByteSequence.fromAccumuloValue") {
    val accumuloValue = new Value("abc")
    val byteSequence = ByteSequence.fromAccumuloValue(accumuloValue)
    byteSequence.toAccumuloValue should equal (accumuloValue)
  }

  test("ByteSequence.fromByteBuffer") {
    val byteBuffer = Charset.forName("UTF-8").encode("abc")
    val byteSequence = ByteSequence.fromByteBuffer(byteBuffer)
    byteSequence.toString should equal ("abc")
  }

  test("ByteSequence.compareTo") {
    ByteSequence.fromString("abc") should be < ByteSequence.fromString("abd")
    ByteSequence.fromString("abc") should be > ByteSequence.fromString("aaa")
    ByteSequence.fromString("abc") should be > ByteSequence.fromString("a")
    ByteSequence.fromString("abc") should be < ByteSequence.fromString("abcd")
    ByteSequence.fromString("abc") should be <= ByteSequence.fromString("abc")
  }

  test("ByteSequence.equals") {
    val bs = ByteSequence.fromString("abc")
    bs should equal (bs)
    bs should equal (ByteSequence.fromString("abc"))
    bs should not equal ("abc")
    bs should not equal (ByteSequence.fromString("abd"))
  }

  test("ByteSequence.hashCode") {
    ByteSequence.fromString("abc").hashCode() should equal (ByteSequence.fromString("abc").hashCode())
    ByteSequence.fromString("abc").hashCode() should not equal (ByteSequence.fromString("abd").hashCode())
  }

  test("ByteSequence.get") {
    val byteSequence = ByteSequence.fromByteArray(Array(1, 2, 3))
    byteSequence.get(0) should equal (1)
    assertThrows[IndexOutOfBoundsException] {
      byteSequence.get(3)
    }
  }

}
