package io.github.reggert.cumulative.core.lexicoders

import java.lang
import java.time.Instant

import org.apache.accumulo.core.client.lexicoder.{IntegerLexicoder, Lexicoder, LongLexicoder}


/**
  * Lexicoder for [[Instant]]. Records with full nanosecond precision.
  */
object InstantLexicoder{
  private implicit val longLexicoder = new LongLexicoder
  private implicit val integerLexicoder = new IntegerLexicoder
  private implicit val pairLexicoder = new Tuple2Lexicoder[java.lang.Long, java.lang.Integer]()

  /**
    * Creates a new instance of this lexicoder.
    */
  def apply() : Lexicoder[Instant] = LexicoderAdapter[Instant, (java.lang.Long, java.lang.Integer)](
    forward = (a: Instant) => (a.getEpochSecond, a.getNano),
    backward = (b: (lang.Long, Integer)) => Instant.ofEpochSecond(b._1, b._2.longValue())
  )
}
