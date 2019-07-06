package io.github.reggert.cumulative.core

import scala.util.matching.Regex


/**
  * Immutable, type-safe, serializable representation of an Accumulo table namespace.
  *
  * @param toString string value of the namespace; may not contain whitespace or dots.
  */
@SerialVersionUID(1L)
final case class Namespace(override val toString : String) {
  require(Namespace.Valid.unapplySeq(toString).nonEmpty, s"Invalid namespace: $toString")
}


object Namespace {
  /**
    * Namespaces may not contain spaces or dots.
    */
  val Valid: Regex = "([^\\.\\s]*)".r

  /**
    * The default namespace ("").
    */
  val Default = Namespace("")

  /**
    * The system namespace ("accumulo"), containing the root and metadata tables.
    */
  val System = Namespace("accumulo")

  implicit val DefaultOrdering : Ordering[Namespace] = Ordering.by(_.toString)
}
