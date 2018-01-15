package io.github.reggert.cumulative.core.scan.iterators

import org.apache.accumulo.core.iterators.user.WholeRowIterator


/**
  * Constructs an [[IteratorConfiguration]] for [[WholeRowIterator]].
  */
object WholeRow {
  /**
    * Creates a new [[IteratorConfiguration]] configured for [[WholeRowIterator]].
    *
    * @param name the name to apply to the iterator; defaults to "WholeRowIterator".
    * @return a new [[IteratorConfiguration]].
    */
  def apply(name : String = "WholeRowIterator"):  IteratorConfiguration =
    new IteratorConfiguration(name, classOf[WholeRowIterator].getName)
}
