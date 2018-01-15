package io.github.reggert.cumulative.core.lexicoders

import org.apache.accumulo.core.client.lexicoder.Lexicoder

import scala.collection.generic.CanBuildFrom


/**
  * Implicit lexicoders for generic types. These typically rely on lexicoders for other types.
  */
trait GenericImplicits {
  implicit def seqLexicoder[A, C[_] <: Seq[_]](implicit
    lex : Lexicoder[A],
    cbf : CanBuildFrom[Nothing, A, C[A]]
  ) : Lexicoder[C[A]] = SeqLexicoder[A, C]
  implicit def tuple2Lexicoder[A : Lexicoder, B : Lexicoder] = new Tuple2Lexicoder[A, B]
  implicit def tuple3Lexicoder[A : Lexicoder, B : Lexicoder, C : Lexicoder] = new Tuple3Lexicoder[A, B, C]
  implicit def tuple4Lexicoder[A : Lexicoder, B : Lexicoder, C : Lexicoder, D : Lexicoder] =
    new Tuple4Lexicoder[A, B, C, D]
}


object GenericImplicits extends GenericImplicits
