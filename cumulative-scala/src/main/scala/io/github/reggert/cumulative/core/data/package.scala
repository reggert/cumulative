package io.github.reggert.cumulative.core

import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.accumulo.core.util.BadArgumentException

import scala.collection.immutable


package object data {
  /**
    * Immutable representation of an Accumulo rowid.
    */
  final case class RowIdentifier(bytes : immutable.IndexedSeq[Byte]) extends ByteSeqWrapper
  object RowIdentifier extends ByteSeqWrapperFactory[RowIdentifier]

  /**
    * Immutable representation of an Accumulo column family.
    * @param bytes value as a sequence of bytes.
    */
  final case class ColumnFamily(bytes : immutable.IndexedSeq[Byte] = Vector()) extends ByteSeqWrapper
  object ColumnFamily extends ByteSeqWrapperFactory[ColumnFamily]

  /**
    * Immutable representation of an Accumulo column qualifier.
    * @param bytes value as a sequence of bytes.
    */
  final case class ColumnQualifier(bytes : immutable.IndexedSeq[Byte] = Vector()) extends ByteSeqWrapper
  object ColumnQualifier extends ByteSeqWrapperFactory[ColumnQualifier]

  /**
    * Immutable representation of an Accumulo entry value.
    * @param bytes value as a sequence of bytes.
    */
  final case class EntryValue(bytes : immutable.IndexedSeq[Byte] = Vector()) extends ByteSeqWrapper
  object EntryValue extends ByteSeqWrapperFactory[EntryValue]

  /**
    * Immutable representation of an Accumulo entry visibility.
    * @param bytes value as a sequence of bytes.
    */
  final case class EntryVisibility(bytes : immutable.IndexedSeq[Byte] = Vector()) extends ByteSeqWrapper {
    /**
      * Validates that the value is a visibility expression.
      * @return an Accumulo [[ColumnVisibility]] object.
      * @throws BadArgumentException if the expression is invalid.
      */
    def parsed : ColumnVisibility = new ColumnVisibility(toArray)
  }
  object EntryVisibility extends ByteSeqWrapperFactory[EntryVisibility]


}
