package io.github.reggert.cumulative.core.lexicoders

import org.apache.accumulo.core.client.lexicoder._


/**
  * Default set of implicitly available [[Lexicoder]]s as a mix-in.
  */
trait Implicits extends BasicImplicits with OptionalImplicits with GenericImplicits with DateTimeImplicits

/**
  * Default set of implicitly available [[Lexicoder]]s as an importable singleton.
  */
object Implicits extends Implicits
