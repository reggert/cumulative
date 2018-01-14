package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data.{ColumnFamily, ColumnQualifier}


/**
  * Immutable representation of a column or column family to include in a scan.
  */
final case class ColumnSelector(family : ColumnFamily, qualifier : Option[ColumnQualifier])
