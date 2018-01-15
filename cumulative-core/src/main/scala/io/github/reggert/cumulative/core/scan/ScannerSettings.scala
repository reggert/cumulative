package io.github.reggert.cumulative.core.scan

import java.util.concurrent.TimeUnit

import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, AccumuloInputFormat, InputFormatBase}
import org.apache.accumulo.core.client.{Scanner, ScannerBase}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapreduce.Job

import scala.concurrent.duration.Duration


/**
  * Base class for containers of settings to pass to Accumulo scanners.
  */
sealed trait ScannerSettings extends Serializable {
  def timeout : Duration
  def batchTimeout : Duration
  def classLoaderContext : Option[String]
  def authorizations : Authorizations

  /**
    * Applies these settings to a scanner.
    * @param scanner scanner to configure.
    */
  final def apply(scanner : ScannerBase) : Unit = {
    if (timeout.isFinite()) {
      scanner.setTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
    }
    if (batchTimeout.isFinite()) {
      scanner.setBatchTimeout(batchTimeout.toMillis, TimeUnit.MILLISECONDS)
    }
    classLoaderContext.foreach(scanner.setClassLoaderContext)
  }

  /**
    * Applies these settings to a Hadoop job configuration (for use with [[AccumuloInputFormat]]).
    * @param configuration Hadoop job configuration to which to apply settings.
    */
  def apply(configuration : Job) : Unit = {
    classLoaderContext.foreach(AbstractInputFormat.setClassLoaderContext(configuration, _))
    AbstractInputFormat.setScanAuthorizations(configuration, authorizations)
  }
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

    /**
      * Applies these settings to a [[Scanner]].
      *
      * @param scanner the scanner to configure.
      */
    def apply(scanner : Scanner) : Unit = {
      super.apply(scanner)
      scanner.setBatchSize(batchSize)
      scanner.setReadaheadThreshold(readAheadThreshold)
      if (isolationEnabled) {
        scanner.enableIsolation()
      }
    }

    override def apply(configuration : Job) : Unit = {
      super.apply(configuration)
      InputFormatBase.setScanIsolation(configuration, isolationEnabled)
    }
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


