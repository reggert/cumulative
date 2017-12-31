package io.github.reggert.cumulative.core.data

import java.time.Instant

import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class TimestampTest extends FunSuite with Matchers {

  test("Timestamp.fromInstant") {
    val instant = Instant.ofEpochMilli(1234567L)
    val timestamp = Timestamp.fromInstant(instant)
    timestamp.toInstant should equal (instant)
  }

  test("Timestamp.fromAccumuloKey") {
    val accumuloKey = new Key(new Text("a"), new Text(""), new Text(""), new Text(""), 12345L)
    val timestamp = Timestamp.fromAccumuloKey(accumuloKey)
    timestamp.longValue() should equal (accumuloKey.getTimestamp)
  }

  test("Timestamp.isSpecified") {
    Timestamp.fromInstant(Instant.ofEpochMilli(123456L)).isSpecified should be (true)
    Timestamp.UNSPECIFIED.isSpecified should be (false)
  }

  test("Timestamp.toString") {
    val instant = Instant.ofEpochMilli(1234567L)
    Timestamp.fromInstant(instant).toString should equal (instant.toString)
    Timestamp.UNSPECIFIED.toString should equal ("UNSPECIFIED")
  }

  test("Timestamp.equals") {
    Timestamp.UNSPECIFIED should equal (Timestamp.UNSPECIFIED)
    Timestamp.UNSPECIFIED should equal (new Timestamp(Long.MaxValue))
    Timestamp.UNSPECIFIED should not equal (Timestamp.fromInstant(Instant.EPOCH))
    Timestamp.UNSPECIFIED should not equal ("abc")
  }

  test("Timestamp.compareTo") {
    Timestamp.UNSPECIFIED should be > Timestamp.fromInstant(Instant.EPOCH)
    Timestamp.UNSPECIFIED should be <= Timestamp.UNSPECIFIED
    new Timestamp(1L) should be < new Timestamp(2L)
  }

  test("Timestamp.hashCode") {
    Timestamp.UNSPECIFIED.hashCode() should equal (new Timestamp(Long.MaxValue).hashCode())
    Timestamp.UNSPECIFIED.hashCode() should not equal (new Timestamp(0L).hashCode())
  }

  test("Timestamp.toInstant") {
    new Timestamp(0L).toInstant should equal (Instant.EPOCH)
    assertThrows[IllegalStateException] {
      Timestamp.UNSPECIFIED.toInstant
    }
  }
}
