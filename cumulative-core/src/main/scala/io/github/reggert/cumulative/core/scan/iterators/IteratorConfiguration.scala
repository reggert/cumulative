package io.github.reggert.cumulative.core.scan.iterators

import org.apache.accumulo.core.client.IteratorSetting

import scala.collection.JavaConverters._


/**
  * Immutable representation of the configuration for an Accumulo iterator.
  */
final case class IteratorConfiguration(
  name : String,
  iteratorClass : String,
  options : Map[String, String] = Map.empty
) extends Serializable {
  /**
    * Constructs an [[IteratorSetting]] with the specified priority.
    *
    * @param priority the priority to assign the iterator.
    * @return an Accumulo [[IteratorSetting]].
    */
  def toIteratorSetting(priority : IteratorConfiguration.Priority) : IteratorSetting =
    new IteratorSetting(priority.intValue, name, iteratorClass, options.asJava)
}


object IteratorConfiguration {

  /**
    * Priority to assign to an iterator.
    *
    * @param intValue integer priority value.
    */
  final case class Priority(intValue : Int) extends AnyVal with Serializable

  val MaxPriority = Priority(Int.MaxValue)
}
