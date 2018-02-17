package io.github.reggert.cumulative.core.scan

import io.github.reggert.cumulative.core.data.{ColumnFamily, ColumnQualifier}
import org.apache.accumulo.core.client.ScannerBase


/**
  * Immutable representation of a column or column family to include in a scan.
  *
  * @param family the column family to select.
  * @param qualifier the optional column qualifier to select; if `None`, the entire family is selected.
  */
final case class ColumnSelector(family : ColumnFamily, qualifier : Option[ColumnQualifier]) {
  /**
    * Configures the specified scanner to fetch the column(s) represented by this selector.
    *
    * @param scanner the Accumulo scanner to configure.
    */
  def apply(scanner: ScannerBase) : Unit = qualifier match {
    case Some(cq) => scanner.fetchColumn(family.toHadoopText, cq.toHadoopText)
    case _ => scanner.fetchColumnFamily(family.toHadoopText)
  }
}
