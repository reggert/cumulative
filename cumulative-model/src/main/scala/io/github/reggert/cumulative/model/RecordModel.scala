package io.github.reggert.cumulative.model

import io.github.reggert.cumulative.core.data.{Entry, Row}
import io.github.reggert.cumulative.core.scan.ColumnSelector

import scala.collection.immutable


/**
  * Base trait for mappings from domain object records to raw Accumulo entries/rows.
  *
  * @tparam D the domain object type.
  * @tparam R the raw Accumulo type (either [[Entry]] or ([[Row]])
  */
trait RecordModel[D, R] {
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
}


/**
  * Interface representing a one-to-one mapping between application domain objects and individual Accumulo
  * entries.
  *
  * @tparam D the domain object type.
  */
trait EntryModel[D] extends RecordModel[D, Entry]


/**
  * Interface representing a one-to-one mapping between application domain objects and entire Accumulo
  * rows.
  *
  * @tparam D the domain object type.
  */
trait RowModel[D] extends RecordModel[D, Row]
