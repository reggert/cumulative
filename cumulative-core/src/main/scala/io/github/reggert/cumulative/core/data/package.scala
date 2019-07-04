package io.github.reggert.cumulative.core

import java.time.Instant

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

  /**
    * Immutable representation of an entire column id (family and qualifier).
    * @param family major key.
    * @param qualifier minor key.
    */
  final case class ColumnIdentifier(
    family : ColumnFamily = ColumnFamily(),
    qualifier : ColumnQualifier = ColumnQualifier()
  )
  object ColumnIdentifier {
    implicit val columnIdentifierOrdering : Ordering[ColumnIdentifier] =
      Ordering.by {id => (id.family, id.qualifier)}
  }

  /**
    * Immutable representation of an Accumulo timestamp, which may represent either an actual time or a
    * logical time, and may be left unspecified (e.g., when writing new entries).
    */
  sealed abstract class Timestamp extends Serializable {
    /**
      * Indicates whether the timestamp has a specified value.
      */
    def isSpecified : Boolean
  }
  object Timestamp {

    /**
      * Immutable representation of an Accumulo timestamp that has been specified.
      * @param longValue numeric value of the timestamp, which may be either Unix time or a logical time.
      */
    final case class Specified(longValue : Long) extends Timestamp {
      /**
        * Converts this timestamp to an [[Instant]], assuming it does not represent a logical time.
        * @return an Instant.
        */
      def toInstant: Instant = Instant.ofEpochMilli(longValue)

      override def isSpecified: Boolean = true
    }

    /**
      * Placeholder for a timestamp that has been left unspecified.
      */
    final case object Unspecified extends Timestamp {
      override def isSpecified: Boolean = false
    }

    /**
      * Constructs a timestamp from an [[Instant]].
      * @param instant an Instant representing the time.
      * @return a new specified timestamp.
      */
    def apply(instant : Instant) = Specified(instant.toEpochMilli)

    /**
      * Default ordering for timestamps. Note that unspecified timestamps sort higher than specified ones.
      */
    //noinspection ConvertExpressionToSAM
    // Using SAM prevents cross compiling with earlier versions of Scala.
    implicit val timestampOrdering : Ordering[Timestamp] = new Ordering[Timestamp] {
      override def compare (x: Timestamp, y: Timestamp): Int = (x, y) match {
        case (Specified(a), Specified(b)) => implicitly[Ordering[Long]].compare(a, b)
        case (Unspecified, Specified(_)) => 1
        case (Specified(_), Unspecified) => -1
        case (Unspecified, Unspecified) => 0
      }
    }
  }

}
