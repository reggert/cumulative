package io.github.reggert.cumulative.core

import io.github.reggert.cumulative.core.TableName.checkValidPart

import scala.util.matching.Regex


/**
  * Represents a table name with optional namespace.
  */
@SerialVersionUID(1L)
final case class TableName(namespace : String, name : String) {
  checkValidPart(namespace)
  checkValidPart(name)
  require(name.nonEmpty, "Empty table names are not valid")

  override def toString: String =
    if (namespace.isEmpty)
      name
    else
      s"$namespace.$name"
}


object TableName {
  /**
    * Creates a table name by parsing the namespace and name from the specified string.
    *
    * @param qualifiedName
    * a table name, possibly prefixed with a namespace and dot.
    * @return a table name.
    * @throws IllegalArgumentException if the argument is not in the expected format.
    */
  def apply(qualifiedName : String) : TableName = qualifiedName match {
    case ValidQualifiedName(ns, name) if name.nonEmpty =>
      if (ns == null) TableName(DefaultNamespace, name) else TableName(ns, name)
    case _ =>
      throw new IllegalArgumentException("Invalid qualified table name: " + qualifiedName)
  }

  /**
    * Names and namespaces may not contain whitespace or dots.
    */
  val ValidPart: Regex = "([^\\.\\s]*)".r
  val ValidQualifiedName: Regex = "(?:(?<ns>[^\\.\\s]+)\\.)?(?<name>[^\\.\\s]+)".r
  val DefaultNamespace = ""

  private def checkValidPart(part: String) : Unit = part match {
    case ValidPart(_) => ()
    case _ =>
      throw new IllegalArgumentException(s"'$part' is not a valid table name token")
  }

  implicit val tableNameOrdering : Ordering[TableName] =
    Ordering.by(x => (x.namespace, x.name))
}
