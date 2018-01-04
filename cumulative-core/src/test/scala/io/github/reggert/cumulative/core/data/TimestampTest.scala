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
    timestamp should equal (new Timestamp.Specified(accumuloKey.getTimestamp))
  }

  test("Timestamp.toString") {
    val instant = Instant.ofEpochMilli(1234567L)
    Timestamp.fromInstant(instant).toString should equal (instant.toString)
    Timestamp.UNSPECIFIED.toString should equal ("UNSPECIFIED")
  }

  test("Timestamp.equals") {
    Timestamp.UNSPECIFIED should equal (Timestamp.UNSPECIFIED)
    Timestamp.UNSPECIFIED should not equal (Timestamp.fromInstant(Instant.EPOCH))
    Timestamp.UNSPECIFIED should not equal ("abc")
    val specified = new Timestamp.Specified(1L)
    specified should equal (specified)
    specified should equal (new Timestamp.Specified(1L))
    specified should not equal (new Timestamp.Specified(2L))
    specified should not equal ("abc")
  }

  test("Timestamp.Specified.compareTo") {
    new Timestamp.Specified(1L) should be < new Timestamp.Specified(2L)
  }

  test("Timestamp.hashCode") {
    Timestamp.UNSPECIFIED.hashCode() should not equal (new Timestamp.Specified(0L).hashCode())
    new Timestamp.Specified(1L).hashCode() should equal (new Timestamp.Specified(1L).hashCode())
  }

  test("Timestamp.Specified.toInstant") {
    new Timestamp.Specified(0L).toInstant should equal (Instant.EPOCH)
  }
}
