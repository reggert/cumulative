package io.github.reggert.cumulative.core.lexicoders

import java.time.Instant

import io.github.reggert.cumulative.core.lexicoders.BasicImplicits._
import io.github.reggert.cumulative.core.lexicoders.GenericImplicits._


/**
  * Lexicoder for [[Instant]]. Records with full nanosecond precision.
  */
object InstantLexicoder extends LexicoderAdapter[Instant, (Long, Int)] {
  override def mapForward(a: Instant): (Long, Int) = (a.getEpochSecond, a.getNano)
  override def mapBackward(b: (Long, Int)): Instant = Instant.ofEpochSecond(b._1, b._2.longValue())
}
