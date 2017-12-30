package io.github.reggert.cumulative.core

import org.scalatest.{FunSuite, Matchers}

import scala.collection.immutable.HashSet


class TableNameTest extends FunSuite with Matchers {

  test("TableName constructor accepts a valid namespace and name") {
    val ns = "testns"
    val name = "testname"
    val tableName = new TableName(ns, name)
    tableName.getNamespace should equal (ns)
    tableName.getName should equal (name)
  }

  test("TableName constructor accepts an empty namespace and valid name") {
    val ns = ""
    val name = "testname"
    val tableName = new TableName(ns, name)
    tableName.getNamespace should equal (ns)
    tableName.getName should equal (name)
  }

  test("TableName constructor rejects an invalid namespace") {
    val ns = "1-.d"
    val name = "testname"
    assertThrows[IllegalArgumentException] {
      new TableName(ns, name)
    }
  }

  test("TableName constructor rejects an invalid name") {
    val ns = ""
    val name = "@@@."
    assertThrows[IllegalArgumentException] {
      new TableName(ns, name)
    }
  }

  test("TableName constructor rejects an empty name") {
    val ns = ""
    val name = ""
    assertThrows[IllegalArgumentException] {
      new TableName(ns, name)
    }
  }

  test("TableName.parse accepts a valid plain name") {
    val tableName = TableName.parse("testname")
    tableName.getNamespace shouldBe empty
    tableName.getName should equal ("testname")
  }

  test("TableName.parse accepts a valid qualified name") {
    val tableName = TableName.parse("testns.testname")
    tableName.getNamespace should equal ("testns")
    tableName.getName should equal ("testname")
  }

  test("TableName.parse rejects an invalid qualified name") {
    assertThrows[IllegalArgumentException] {
      TableName.parse("...")
    }
  }

  test("TableName.compareTo sorts by namespace first") {
    TableName.parse("a.y") should be < (TableName.parse("b.x"))
    TableName.parse("c.x") should be > (TableName.parse("b.z"))
  }

  test("TableName.compareTo sorts by name second") {
    TableName.parse("a.x") should be < (TableName.parse("a.y"))
    TableName.parse("a.z") should be > (TableName.parse("a.y"))
  }

  test("TableName.equals") {
    TableName.parse("a.x") should equal (TableName.parse("a.x"))
    val x = TableName.parse("x")
    x should equal (x)
    x should not equal ("x")
    TableName.parse("a.x") should not equal (TableName.parse("b.x"))
    TableName.parse("a.x") should not equal (TableName.parse("a.y"))
  }

  test("TableName.toString") {
    TableName.parse("a.x").toString should equal ("a.x")
    TableName.parse("x").toString should equal ("x")
  }

  test("TableName.hashCode") {
    val s = HashSet(TableName.parse("a.x"), TableName.parse("b.y"))
    s.contains(TableName.parse("a.x")) should be (true)
  }
}
