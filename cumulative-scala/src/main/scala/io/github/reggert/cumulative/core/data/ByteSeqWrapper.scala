package io.github.reggert.cumulative.core.data

import java.nio.ByteBuffer
import java.nio.charset.Charset

import io.github.reggert.cumulative.core.data.ByteSeqWrapper.UTF8
import org.apache.accumulo.core.client.lexicoder.Lexicoder
import org.apache.accumulo.core.data.{ByteSequence, Value}
import org.apache.hadoop.io.Text

import scala.collection.immutable
import scala.util.Try


/**
  * Operations on wrappers around byte sequences.
  */
trait ByteSeqWrapper {
  /**
    * Underlying byte sequence.
    */
  def bytes : immutable.IndexedSeq[Byte]

  /**
    * Converts the underlying byte sequence to array.
    */
  final def toArray : Array[Byte] = bytes.toArray

  /**
    * Converts the underlying byte sequence to a Hadoop [[Text]] object.
    */
  final def toHadoopText : Text = new Text(toArray)

  /**
    * Converts the underlying byte sequence to an Accumulo [[Value]] object.
    */
  final def toAccumuloValue : Value = new Value(toArray)

  /**
    * Decodes the underlying byte sequence as the specified type for which a [[Lexicoder]] is implicitly
    * available.
    * @tparam T type to which to convert.
    * @return the converted value.
    * @throws IllegalArgumentException if the lexicoder is unable to convert the value.
    */
  final def to[T : Lexicoder] : T = implicitly[Lexicoder[T]].decode(toArray)

  /**
    * Decodes the underlying byte sequence as the specified type for which a [[Lexicoder]] is implicitly
    * available, and returns the result as a [[Try]].
    * @tparam T type to which to convert.
    * @return the converted value wrapped in a [[Try]].
    */
  final def decode[T : Lexicoder] : Try[T] = Try(to[T])

  /**
    * Converts the underlying byte sequence to an Accumulo [[ByteSequence]] object.
    */
  final def toAccumuloByteSequence : ByteSequence = new ByteSequence {
    override def byteAt(i: Int): Byte = bytes(i)
    override def offset(): Int = 0
    override def getBackingArray: Array[Byte] = null
    override def length(): Int = bytes.size
    override def subSequence(start: Int, end: Int): ByteSequence =
      ByteSeqWrapper.Simple(bytes.slice(start, end)).toAccumuloByteSequence
    override def toArray: Array[Byte] = bytes.toArray
    override def isBackedByArray: Boolean = false
  }

  /**
    * This implementation interprets the underlying byte sequence as UTF-8 text.
    */
  final override def toString : String =
    UTF8.decode(ByteBuffer.wrap(toArray)).toString
}


object ByteSeqWrapper {
  val UTF8: Charset = Charset.forName("UTF-8")

  implicit def byteSeqWrapperOrdering[T <: ByteSeqWrapper] : Ordering[T] =
    Ordering.by(_.bytes.view.map(_ & 0xff))

  private final case class Simple(bytes : immutable.IndexedSeq[Byte]) extends ByteSeqWrapper
}


/**
  * Base trait for companion objects of classes that extend [[ByteSeqWrapper]].
  * @tparam T the concrete companion class.
  */
trait ByteSeqWrapperFactory[T <: ByteSeqWrapper] {
  def apply(bytes : immutable.IndexedSeq[Byte]) : T
  final def apply(array : Array[Byte]) : T = apply(array.toIndexedSeq)
  final def apply(hadoopText : Text) : T = apply(hadoopText.copyBytes())
  final def apply(accumuloValue : Value) : T = apply(accumuloValue.get())
  final def apply(string : String) : T = apply(string.getBytes(UTF8))
  final def apply(byteSequence : ByteSequence) : T =
    apply((0 until byteSequence.length()).map(byteSequence.byteAt))
  final def apply[A : Lexicoder](v : A) : T = apply(implicitly[Lexicoder[A]].encode(v))
}
