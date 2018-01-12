package io.github.reggert.cumulative.core.data

import java.nio.ByteBuffer
import java.nio.charset.Charset

import io.github.reggert.cumulative.core.data.ByteSeqWrapper.UTF8
import org.apache.accumulo.core.data.Value
import org.apache.hadoop.io.Text

import scala.collection.immutable


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
    * This implementation interprets the underlying byte sequence as UTF-8 text.
    */
  final override def toString : String =
    UTF8.decode(ByteBuffer.wrap(toArray)).toString
}


object ByteSeqWrapper {
  val UTF8: Charset = Charset.forName("UTF-8")

  implicit def byteSeqWrapperOrdering[T <: ByteSeqWrapper] : Ordering[T] =
    Ordering.by(_.bytes.view.map(_ & 0xff))
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
}
