package io.github.reggert.cumulative.core.lexicoders

import org.apache.accumulo.core.client.lexicoder.Lexicoder
import BasicImplicits._


/**
  * Adapters for Scala value types for which Accumulo does not directly provide lexicoders.
  * Users may want to exclude these from imports if alternate encodings are desired.
  */
trait OptionalImplicits {

  implicit lazy val byteLexicoder : Lexicoder[Byte] = LexicoderAdapter[Byte, Array[Byte]](
    Array(_),
    bytes => {require(bytes.length == 1, s"wrong length: ${bytes.length}"); bytes(0)}
  )
  implicit lazy val shortLexicoder : Lexicoder[Short] = LexicoderAdapter[Short, Int](
    {n : Short => n.toInt},
    int => {
      require(int >= Short.MinValue && int <= Short.MaxValue, s"value out of range for Short: $int")
      int.toShort
    }
  )
  implicit lazy val booleanLexicoder : Lexicoder[Boolean] = LexicoderAdapter[Boolean, Byte](
    {
      case true => 1 : Byte
      case false => 0 : Byte
    },
    {byte : Byte => byte match {
      case 0 => false
      case 1 => true
      case _ => throw new IllegalArgumentException(s"Value is not 0 or 1: $byte")
    }}
  )
}


object OptionalImplicits extends OptionalImplicits
