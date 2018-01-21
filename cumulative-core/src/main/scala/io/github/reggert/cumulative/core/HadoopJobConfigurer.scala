package io.github.reggert.cumulative.core

import org.apache.hadoop.mapreduce.Job


/**
  * Interface for objects that configure Hadoop [[Job]] objects.
  */
trait HadoopJobConfigurer extends Serializable {
  /**
    * Applies the object's settings to the specified Hadoop job configuration for use with
    * [[org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat]] and related classes.
    *
    * @param configuration Hadoop job configuration to which to apply settings.
    */
  def configure(configuration : Job) : Unit
}
