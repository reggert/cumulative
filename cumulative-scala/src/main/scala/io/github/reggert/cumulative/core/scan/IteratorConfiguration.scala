package io.github.reggert.cumulative.core.scan

import org.apache.accumulo.core.client.IteratorSetting


/**
  * Immutable representation of the configuration for an Accumulo iterator.
  *
  * The main reason this class exists rather than using {@code IteratorSetting} directly to enable the priorities
  * to be automatically assigned at scan time.
  */
abstract class IteratorConfiguration(
  val name : String,
  val iteratorClass : String,
  val options : Map[String, String] = Map.empty
) extends Serializable {
  /**
    * Constructs an [[IteratorSetting]] with the specified priority.
    *
    * @param priority the priority to assign the iterator.
    * @return an Accumulo [[IteratorSetting]].
    */
  def toIteratorSetting(priority : Int) : IteratorSetting
}
