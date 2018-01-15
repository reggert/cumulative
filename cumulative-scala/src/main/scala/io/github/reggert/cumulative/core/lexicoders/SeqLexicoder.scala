package io.github.reggert.cumulative.core.lexicoders

import org.apache.accumulo.core.client.lexicoder.{Lexicoder, ListLexicoder}
import java.util

import scala.collection.generic.CanBuildFrom
import scala.collection.JavaConverters._
import scala.language.higherKinds


object SeqLexicoder {
  def apply[A, C[_] <: Seq[_]](implicit
    lex : Lexicoder[A],
    canBuildFrom: CanBuildFrom[Nothing, A, C[A]]
  ) : Lexicoder[C[A]] = {
    implicit val listLexicoder : Lexicoder[util.List[A]] = new ListLexicoder[A](lex)
    LexicoderAdapter[C[A], util.List[A]](
      forward = (a : C[A]) => {
        val seq : Seq[A] = a.asInstanceOf[Seq[A]]
        new util.ArrayList[A](seq.asJavaCollection)
      },
      backward = (b : util.List[A]) => b.asScala.to[C]
    )
  }
}
