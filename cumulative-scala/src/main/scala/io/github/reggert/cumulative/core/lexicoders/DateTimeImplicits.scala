package io.github.reggert.cumulative.core.lexicoders

import java.time.Instant

import org.apache.accumulo.core.client.lexicoder.Lexicoder


/**
  * Implicit lexicoders for dealing with `java.time` types.
  */
trait DateTimeImplicits {
  implicit lazy val instantLexicoder : Lexicoder[Instant] = InstantLexicoder
}


object DateTimeImplicits extends DateTimeImplicits
