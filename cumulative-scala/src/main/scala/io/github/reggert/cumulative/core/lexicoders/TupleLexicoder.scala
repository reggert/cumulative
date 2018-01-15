package io.github.reggert.cumulative.core.lexicoders

import org.apache.accumulo.core.client.lexicoder.Lexicoder
import org.apache.accumulo.core.client.lexicoder.impl.ByteUtils._


/**
  * Lexicoder for Scala pairs.
  *
  * @tparam A type of first part of pair.
  * @tparam B type of second part of pair.
  */
class Tuple2Lexicoder[A, B](implicit
  lexicoderA : Lexicoder[A],
  lexicoderB : Lexicoder[B]
) extends Lexicoder[(A, B)] {
  override def encode(v: (A, B)): Array[Byte] = concat(
    escape(lexicoderA.encode(v._1)),
    escape(lexicoderB.encode(v._2))
  )

  override def decode(b: Array[Byte]): (A, B) = {
    val fields = split(b)
    require(fields.length == 2, s"Wrong number of fields (${fields.length}); expected 2")
    (
      lexicoderA.decode(unescape(fields(0))),
      lexicoderB.decode(unescape(fields(1)))
    )
  }
}


/**
  * Lexicoder for Scala triples.
  *
  * @tparam A type of first part of triple.
  * @tparam B type of second part of triple.
  * @tparam C type of third part of triple.
  */
class Tuple3Lexicoder[A, B, C](implicit
  lexicoderA : Lexicoder[A],
  lexicoderB : Lexicoder[B],
  lexicoderC : Lexicoder[C]
) extends Lexicoder[(A, B, C)] {
  override def encode(v: (A, B, C)): Array[Byte] = concat(
    escape(lexicoderA.encode(v._1)),
    escape(lexicoderB.encode(v._2)),
    escape(lexicoderC.encode(v._3))
  )

  override def decode(b: Array[Byte]): (A, B, C) = {
    val fields = split(b)
    require(fields.length == 3, s"Wrong number of fields (${fields.length}); expected 3")
    (
      lexicoderA.decode(unescape(fields(0))),
      lexicoderB.decode(unescape(fields(1))),
      lexicoderC.decode(unescape(fields(2)))
    )
  }
}


/**
  * Lexicoder for Scala quads.
  * @tparam A type of first part of quad.
  * @tparam B type of second part of quad.
  * @tparam C type of third part of quad.
  */
class Tuple4Lexicoder[A, B, C, D](implicit
  lexicoderA : Lexicoder[A],
  lexicoderB : Lexicoder[B],
  lexicoderC : Lexicoder[C],
  lexicoderD : Lexicoder[D]
) extends Lexicoder[(A, B, C, D)] {
  override def encode(v: (A, B, C, D)): Array[Byte] = concat(
    escape(lexicoderA.encode(v._1)),
    escape(lexicoderB.encode(v._2)),
    escape(lexicoderC.encode(v._3)),
    escape(lexicoderD.encode(v._4))
  )

  override def decode(b: Array[Byte]): (A, B, C, D) = {
    val fields = split(b)
    require(fields.length == 3, s"Wrong number of fields (${fields.length}); expected 3")
    (
      lexicoderA.decode(unescape(fields(0))),
      lexicoderB.decode(unescape(fields(1))),
      lexicoderC.decode(unescape(fields(2))),
      lexicoderD.decode(unescape(fields(3)))
    )
  }
}
