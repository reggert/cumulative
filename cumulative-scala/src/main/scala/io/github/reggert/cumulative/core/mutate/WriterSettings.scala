package io.github.reggert.cumulative.core.mutate

import java.util.concurrent.TimeUnit

import org.apache.accumulo.core.client.{BatchWriterConfig, ConditionalWriterConfig, Durability}

import scala.concurrent.duration.Duration
import scala.concurrent.duration._


/**
  * Immutable settings to configure write operations.
  * @see [[BatchWriterConfig]]
  * @see [[ConditionalWriterConfig]]
  */
final case class WriterSettings(
  timeout : Duration = Duration.Undefined,
  maxLatency : Duration = 2.minutes,
  maxMemory : Long = 50L * 1024L * 1024L,
  maxWriteThreads : Int = 3,
  durability : Durability = Durability.DEFAULT
) {
  /**
    * Converts this object into an Accumulo [[BatchWriterConfig]].
    * @return a new [[BatchWriterConfig]].
    */
  def toBatchWriterConfig : BatchWriterConfig = {
    val config = new BatchWriterConfig
    config.setTimeout(
      if (timeout.isFinite()) timeout.toMillis else Long.MaxValue,
      TimeUnit.MILLISECONDS
    )
    config.setMaxLatency(
      if (maxLatency.isFinite()) maxLatency.toMillis else Long.MaxValue,
      TimeUnit.MILLISECONDS
    )
    config.setMaxMemory(maxMemory)
    config.setMaxWriteThreads(maxWriteThreads)
    config.setDurability(durability)
    config
  }

  /**
    * Converts this object into an Accumulo [[ConditionalWriterConfig]].
    *
    * Note that some settings are ignored.
    *
    * @return a new [[ConditionalWriterConfig]].
    */
  def toConditionalWriterConfig : ConditionalWriterConfig = {
    val config = new ConditionalWriterConfig
    config.setTimeout(
      if (timeout.isFinite()) timeout.toMillis else Long.MaxValue,
      TimeUnit.MILLISECONDS
    )
    config.setMaxWriteThreads(maxWriteThreads)
    config.setDurability(durability)
    config
  }
}
