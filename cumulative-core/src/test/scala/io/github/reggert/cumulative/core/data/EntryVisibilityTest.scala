package io.github.reggert.cumulative.core.data

import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.accumulo.core.util.BadArgumentException
import org.apache.hadoop.io.Text
import org.scalatest.{FunSuite, Matchers}


class EntryVisibilityTest extends FunSuite with Matchers {

  test("EntryVisibility.fromByteSequence") {
    val byteSequence = ByteSequence.fromString("abc")
    val entryVisibility = EntryVisibility.fromByteSequence(byteSequence)
    entryVisibility.toByteSequence should equal (byteSequence)
  }

  test("EntryVisibility.fromByteArray") {
    val originalBytes = Seq[Byte](1, 2, 3)
    val byteArray = originalBytes.toArray
    val entryVisibility = EntryVisibility.fromByteArray(byteArray)
    byteArray(0) = -1
    byteArray(1) = -2
    byteArray(2) = -3
    entryVisibility.toByteArray.toSeq should equal (originalBytes)
  }

  test("EntryVisibility.fromHadoopText") {
    val hadoopText = new Text("abc")
    val entryVisibility = EntryVisibility.fromHadoopText(hadoopText)
    entryVisibility.toHadoopText should equal (hadoopText)
  }

  test("EntryVisibility.fromString") {
    val string = "abc"
    val entryVisibility = EntryVisibility.fromString(string)
    entryVisibility.toString should equal (string)
  }

  test("EntryVisibility.fromAccumuloKey") {
    val key = new Key(new Text("abc"), new Text("def"), new Text("hij"), new Text("klm"))
    val entryVisibility = EntryVisibility.fromAccumuloKey(key)
    entryVisibility.toString should equal ("klm")
  }

  test("EntryVisibility.fromColumnVisibility") {
    val columnVisibility = new ColumnVisibility("abc|def")
    val entryVisibility = EntryVisibility.fromColumnVisiblity(columnVisibility)
    entryVisibility.toString should equal ("abc|def")
  }

  test("EntryVisibility.toColumnVisibility") {
    EntryVisibility.fromString("abc|def").toColumnVisibility should equal (new ColumnVisibility("abc|def"))
    assertThrows[BadArgumentException] {
      EntryVisibility.fromString("(((").toColumnVisibility
    }
  }

  test("EntryVisibility.compareTo") {
    EntryVisibility.fromString("abc") should be < EntryVisibility.fromString("abd")
    EntryVisibility.fromString("abc") should be > EntryVisibility.fromString("aaa")
    EntryVisibility.fromString("abc") should be > EntryVisibility.fromString("a")
    EntryVisibility.fromString("abc") should be < EntryVisibility.fromString("abcd")
    EntryVisibility.fromString("abc") should be <= EntryVisibility.fromString("abc")
  }

  test("EntryVisibility.equals") {
    val bs = EntryVisibility.fromString("abc")
    bs should equal (bs)
    bs should equal (EntryVisibility.fromString("abc"))
    bs should not equal ("abc")
    bs should not equal (EntryVisibility.fromString("abd"))
  }

  test("EntryVisibility.hashCode") {
    EntryVisibility.fromString("abc").hashCode() should equal (EntryVisibility.fromString("abc").hashCode())
    EntryVisibility.fromString("abc").hashCode() should not equal (EntryVisibility.fromString("abd").hashCode())
  }

}
