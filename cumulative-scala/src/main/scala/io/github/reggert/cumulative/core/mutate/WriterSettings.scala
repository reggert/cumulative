package io.github.reggert.cumulative.core.mutate

import java.util.concurrent.TimeUnit

import org.apache.accumulo.core.client.{BatchWriterConfig, Durability}

import scala.concurrent.duration.Duration
import scala.concurrent.duration._


/**
  * Immutable settings to configure write operations.
  * @see [[BatchWriterConfig]]
  */
final case class WriterSettings(
  timeout : Duration = Duration.Undefined,
  maxLatency : Duration = 2.minutes,
  maxMemory : Long = 50L * 1024L * 1024L,
  maxWriteThreads : Int = 3,
  durability : Durability = Durability.DEFAULT
) {
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
}
