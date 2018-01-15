package io.github.reggert.cumulative.core.lexicoders

import org.apache.accumulo.core.client.lexicoder.Lexicoder


/**
  * Adapter that implements a [[Lexicoder]] for a type that can be converted to and from another type for
  * which a `Lexicoder` already exists.
  */
abstract class LexicoderAdapter[A, B : Lexicoder] extends Lexicoder[A] {
  private def delegate = implicitly[Lexicoder[B]]

  override final def encode(value : A): Array[Byte] = delegate.encode(mapForward(value))

  override final def decode(bytes : Array[Byte]): A = mapBackward(delegate.decode(bytes))

  /**
    * Converts from this `Lexicoder`'s value type to the value type of the `Lexicoder` to which encoding will
    * be delegated.
    *
    * @param a the value to be encoded.
    * @return the value to be passed to the delegated `Lexicoder`.
    */
  def mapForward(a : A) : B

  /**
    * Converts to this `Lexicoder`'s value type from the value type of the `Lexicoder` to which decoding was
    * delegated.
    *
    * @param b the value returned by the delegated `Lexicoder`.
    * @return the value to be returned by this `Lexicoder`.
    */
  def mapBackward(b : B) : A
}


object LexicoderAdapter {
  /**
    * Constructs a [[Lexicoder]] based on the specified conversion functions to and from a type for which a
    * `Lexicoder` is implicitly available.
    *
    * @param forward conversion function to the delegate type.
    * @param backward conversion function from the delegate type.
    * @tparam A type that the resultant `Lexicoder` will encode.
    * @tparam B type of the `Lexicoder` to which encoding/decoding will be delegated.
    * @return a new `Lexicoder`.
    */
  def apply[A, B : Lexicoder](forward : A => B, backward : B => A) : Lexicoder[A] = new LexicoderAdapter[A, B] {
    override def mapForward(a: A): B = forward(a)
    override def mapBackward(b: B): A = backward(b)
  }
}
