package io.github.reggert.cumulative.core.lexicoders

import java.time._

import io.github.reggert.cumulative.core.lexicoders.BasicImplicits._
import io.github.reggert.cumulative.core.lexicoders.GenericImplicits._


/**
  * Lexicoder for [[Instant]]. Records with full nanosecond precision.
  */
object InstantLexicoder extends LexicoderAdapter[Instant, (Long, Int)] {
  override def mapForward(a: Instant): (Long, Int) = (a.getEpochSecond, a.getNano)
  override def mapBackward(b: (Long, Int)): Instant = Instant.ofEpochSecond(b._1, b._2.longValue())
}


/**
  * Lexicoder for [[LocalDate]].
  */
object LocalDateLexicoder extends LexicoderAdapter[LocalDate, Long] {
  override def mapForward(a: LocalDate): Long = a.toEpochDay
  override def mapBackward(b: Long): LocalDate = LocalDate.ofEpochDay(b)
}


/**
  * Lexicoder for [[LocalTime]]. Records with full nanosecond precision.
  */
object LocalTimeLexicoder extends LexicoderAdapter[LocalTime, Long] {
  override def mapForward(a: LocalTime): Long = a.toNanoOfDay
  override def mapBackward(b: Long): LocalTime = LocalTime.ofNanoOfDay(b)
}


/**
  * Lexicoder for [[LocalDateTime]]. Records with full nanosecond precision.
  */
object LocalDateTimeLexicoder extends LexicoderAdapter[LocalDateTime, (Long, Long)] {
  override def mapForward(a: LocalDateTime): (Long, Long) =
    (a.toLocalDate.toEpochDay, a.toLocalTime.toNanoOfDay)
  override def mapBackward(b: (Long, Long)): LocalDateTime =
    LocalDateTime.of(LocalDate.ofEpochDay(b._1), LocalTime.ofNanoOfDay(b._2))
}


/**
  * Lexicoder for [[Duration]]. Records with full nanosecond precision.
  */
object DurationLexicoder extends LexicoderAdapter[Duration, (Long, Int)] {
  override def mapForward(a: Duration): (Long, Int) = (a.getSeconds, a.getNano)
  override def mapBackward(b: (Long, Int)): Duration = Duration.ofSeconds(b._1, b._2)
}
