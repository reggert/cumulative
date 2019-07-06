package io.github.reggert.cumulative.core

import org.scalatest.{FunSuite, Matchers}


/**
  * Unit tests for [[TableName]].
  */
class TableNameTest extends FunSuite with Matchers {

  test("TableName constructor accepts a valid namespace and name") {
    val ns = Namespace("testns")
    val name = "testname"
    val tableName = TableName(ns, name)
    tableName.namespace should equal (ns)
    tableName.name should equal (name)
  }

  test("TableName constructor accepts an empty namespace and valid name") {
    val ns = Namespace.Default
    val name = "testname"
    val tableName = TableName(ns, name)
    tableName.namespace should equal (ns)
    tableName.name should equal (name)
  }

  test("Namespace constructor rejects an invalid namespace") {
    val ns = "1-.d"
    assertThrows[IllegalArgumentException] {
      Namespace(ns)
    }
  }

  test("TableName constructor rejects an invalid name") {
    val ns = Namespace.Default
    val name = "@@@."
    assertThrows[IllegalArgumentException] {
      TableName(ns, name)
    }
  }

  test("TableName constructor rejects an empty name") {
    val ns = Namespace.Default
    val name = ""
    assertThrows[IllegalArgumentException] {
      TableName(ns, name)
    }
  }

  test("TableName.apply accepts a valid plain name") {
    val tableName = TableName("testname")
    tableName.namespace should equal (Namespace.Default)
    tableName.name should equal ("testname")
  }

  test("TableName.apply accepts a valid qualified name") {
    val tableName = TableName("testns.testname")
    tableName.namespace should equal ("testns")
    tableName.name should equal ("testname")
  }

  test("TableName.apply rejects an invalid qualified name") {
    assertThrows[IllegalArgumentException] {
      TableName("...")
    }
  }

  test("TableName.tableNameOrdering sorts by namespace first") {
    TableName("a.y") should be < TableName("b.x")
    TableName("c.x") should be > TableName("b.z")
  }

  test("TableName.tableNameOrdering sorts by name second") {
    TableName("a.x") should be < TableName("a.y")
    TableName("a.z") should be > TableName("a.y")
  }

  test("TableName.toString") {
    TableName("a.x").toString should equal ("a.x")
    TableName("x").toString should equal ("x")
  }
}
