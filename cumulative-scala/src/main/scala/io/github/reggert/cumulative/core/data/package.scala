package io.github.reggert.cumulative.core

import scala.collection.immutable


package object data {
  /**
    * Immutable representation of an Accumulo rowid.
    */
  final case class RowIdentifier(bytes : immutable.IndexedSeq[Byte]) extends ByteSeqWrapper
  object RowIdentifier extends ByteSeqWrapperFactory[RowIdentifier]

  /**
    * Immutable representation of an Accumulo column family..
    */
  final case class ColumnFamily(bytes : immutable.IndexedSeq[Byte] = Vector()) extends ByteSeqWrapper
  object ColumnFamily extends ByteSeqWrapperFactory[ColumnFamily]

  final case class ColumnQualifier(bytes : immutable.IndexedSeq[Byte] = Vector()) extends ByteSeqWrapper
  object ColumnQualifier extends ByteSeqWrapperFactory[ColumnQualifier]

}
