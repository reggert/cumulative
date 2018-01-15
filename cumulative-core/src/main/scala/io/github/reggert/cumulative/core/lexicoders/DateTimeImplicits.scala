package io.github.reggert.cumulative.core.lexicoders

import java.time._

import org.apache.accumulo.core.client.lexicoder.Lexicoder


/**
  * Implicit lexicoders for dealing with `java.time` types.
  */
trait DateTimeImplicits {
  implicit val instantLexicoder : Lexicoder[Instant] = InstantLexicoder
  implicit val localDateLexicoder : Lexicoder[LocalDate] = LocalDateLexicoder
  implicit val localTimeLexicoder : Lexicoder[LocalTime] = LocalTimeLexicoder
  implicit val localDateTimeLexicoder : Lexicoder[LocalDateTime] = LocalDateTimeLexicoder
  implicit val durationLexicoder : Lexicoder[Duration] = DurationLexicoder
}


object DateTimeImplicits extends DateTimeImplicits
