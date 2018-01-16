package io.github.reggert.cumulative.model

import io.github.reggert.cumulative.core.data.{Entry, Row}
import io.github.reggert.cumulative.core.scan.{ColumnSelector, ScanRange}

import scala.collection.immutable


/**
  * Base trait for mappings from domain object records to raw Accumulo entries/rows.
  *
  * @tparam D the domain object type.
  * @tparam R the raw Accumulo type (either [[Entry]] or ([[Row]])
  * @tparam B the base type of scan ranges allowed by this model.
  */
sealed trait RecordModel[D, R, B <: ScanRange] extends Serializable {
  /**
    * Converts an application domain object into its raw Accumulo representation.
    *
    * @param domainObject the domain object to convert.
    * @return the Accumulo representation of the object.
    */
  def domainToRaw(domainObject : D) : R

  /**
    * Creates an application domain object from its raw Accumulo representation.
    *
    * @param raw the Accumulo representation of the object.
    * @return the converted domain object.
    * @throws RuntimeException if the object cannot be created from the Accumulo data.
    */
  def domainFromRaw(raw : R) : D

  /**
    * If non-empty, this limits the columns that should be included when scanning, restricting the data
    * that will be passed to `domainFromRaw`.
    *
    * @return an immutable set of [[ColumnSelector]]s, or an empty set to indicate that all columns should
    *         be included.
    */
  def columnSelectors : immutable.Set[ColumnSelector] = Set.empty

  /**
    * Base class for defining domain-specific range types that map into [[ScanRange]]s.
    */
  abstract class RecordRange extends Serializable {
    /**
      * Converts this domain-specific range into a raw [[ScanRange]].
      *
      * @return a `ScanRange` corresponding to the domain range that this object represents.
      */
    def toScanRange : B
  }

  /**
    * Domain-specific equivalent of [[ScanRange.FullTable]].
    */
  object FullTable extends RecordRange {
    override def toScanRange: B = ScanRange.FullTable.asInstanceOf[B]
  }
}


/**
  * Interface representing a one-to-one mapping between application domain objects and individual Accumulo
  * entries.
  *
  * @tparam D the domain object type.
  */
trait EntryModel[D] extends RecordModel[D, Entry, ScanRange]


/**
  * Interface representing a one-to-one mapping between application domain objects and entire Accumulo
  * rows.
  *
  * @tparam D the domain object type.
  */
trait RowModel[D] extends RecordModel[D, Row, ScanRange.WholeRow]
