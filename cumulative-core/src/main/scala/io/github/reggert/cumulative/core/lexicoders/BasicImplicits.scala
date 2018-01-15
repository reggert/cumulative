package io.github.reggert.cumulative.core.lexicoders

import java.math.BigInteger
import java.util

import org.apache.accumulo.core.client.lexicoder._
import org.apache.accumulo.core.util.ComparablePair


/**
  * Implicit lexicoders for simple types, available for mix-in.
  */
trait BasicImplicits {
  // Boxed type lexicoders provided by Accumulo.
  implicit lazy val boxedIntegerLexicoder : Lexicoder[Integer] = new IntegerLexicoder
  implicit lazy val boxedFloatLexicoder : Lexicoder[java.lang.Float] = new FloatLexicoder
  implicit lazy val boxedLongLexicoder : Lexicoder[java.lang.Long] = new LongLexicoder
  implicit lazy val boxedDoubleLexicoder : Lexicoder[java.lang.Double] = new DoubleLexicoder

  // Other lexicoders provided by Accumulo.
  implicit def comparablePairLexicoder[A <: Comparable[A] : Lexicoder, B <: Comparable[B] : Lexicoder] : Lexicoder[ComparablePair[A, B]] =
    new PairLexicoder[A, B](implicitly[Lexicoder[A]], implicitly[Lexicoder[B]])
  implicit def javaListLexicoder[A : Lexicoder] : Lexicoder[util.List[A]] =
    new ListLexicoder[A](implicitly[Lexicoder[A]])
  implicit lazy val byteArrayLexicoder : Lexicoder[Array[Byte]] = new BytesLexicoder
  implicit lazy val utilDateLexicoder : Lexicoder[util.Date] = new DateLexicoder
  implicit lazy val bigIntegerLexicoder : Lexicoder[BigInteger] = new BigIntegerLexicoder
  implicit lazy val stringLexicoder : Lexicoder[String] = new StringLexicoder

  // Adapters for Scala value types
  implicit lazy val intLexicoder : Lexicoder[Int] =
    LexicoderAdapter[Int, Integer](Integer.valueOf, _.intValue)
  implicit lazy val longLexicoder : Lexicoder[Long] =
    LexicoderAdapter[Long, java.lang.Long](java.lang.Long.valueOf, _.longValue)
  implicit lazy val floatLexicoder : Lexicoder[Float] =
    LexicoderAdapter[Float, java.lang.Float](java.lang.Float.valueOf, _.floatValue)
  implicit lazy val doubleLexicoder : Lexicoder[Double] =
    LexicoderAdapter[Double, java.lang.Double](java.lang.Double.valueOf, _.doubleValue)
}


/**
  * Implicit lexicoders for simple types, available for import.
  */
object BasicImplicits extends BasicImplicits
