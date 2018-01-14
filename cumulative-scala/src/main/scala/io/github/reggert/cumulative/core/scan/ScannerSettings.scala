package io.github.reggert.cumulative.core.scan

import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.security.Authorizations

import scala.concurrent.duration.Duration

/**
  * Base class for containers of settings to pass to Accumulo scanners.
  */
sealed trait ScannerSettings extends Serializable {
  def timeout : Duration
  def batchTimeout : Duration
  def classLoaderContext : Option[String]
  def authorizations : Authorizations
}


object ScannerSettings {

  /**
    * Settings used by simple scanners.
    */
  final case class Simple(
    timeout: Duration = Duration.Undefined,
    batchTimeout: Duration = Duration.Undefined,
    classLoaderContext: Option[String] = None,
    authorizations: Authorizations = new Authorizations,
    batchSize : Int = Constants.SCAN_BATCH_SIZE,
    readAheadThreshold : Long = Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD,
    isolationEnabled : Boolean = false
  ) extends ScannerSettings {
    require(batchSize > 0, s"batchSize ($batchSize) must be greater than 0")
    require(readAheadThreshold > 0L, s"readAheadThreshold ($readAheadThreshold) must be greater than 0")
  }


  /**
    * Settings used by batch scanners.
    */
  final case class Batch(
    timeout: Duration = Duration.Undefined,
    batchTimeout: Duration = Duration.Undefined,
    classLoaderContext: Option[String] = None,
    authorizations: Authorizations = new Authorizations,
    numberOfQueryThreads : Int = 2
  ) extends ScannerSettings

}


