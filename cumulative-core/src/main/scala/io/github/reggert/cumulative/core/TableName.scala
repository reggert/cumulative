package io.github.reggert.cumulative.core

import scala.util.matching.Regex


/**
  * Represents a table name with optional namespace.
  */
@SerialVersionUID(1L)
final case class TableName(namespace : Namespace, name : String) {
  require(TableName.ValidName.unapplySeq(name).nonEmpty, s"Invalid table name: $name")

  override def toString: String =
    if (namespace == Namespace.Default)
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
    case ValidQualifiedName(ns, name) =>
      if (ns == null) TableName(Namespace.Default, name) else TableName(Namespace(ns), name)
    case _ =>
      throw new IllegalArgumentException("Invalid qualified table name: " + qualifiedName)
  }

  /**
    * Names may not contain whitespace or dots.
    */
  val ValidName: Regex = "([^\\.\\s]*)".r
  val ValidQualifiedName: Regex = "(?:(?<ns>[^\\.\\s]+)\\.)?(?<name>[^\\.\\s]+)".r

  implicit val tableNameOrdering : Ordering[TableName] =
    Ordering.by(x => (x.namespace, x.name))
}
