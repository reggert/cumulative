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
  /**
    * The timeout for retries in the event of I/O failures; may be undefined/infinite to retry indefinitely.
    */
  val timeout : Duration

  /**
    * The amount of time to wait for the batch buffer to fill before returning the results; may be
    * undefined/infinite to always wait for the buffer to completely fill.
    */
  val batchTimeout : Duration

  /**
    * Optional classloader context to pass to the underlying scanner.
    */
  val classLoaderContext : Option[String]

  /**
    * Scan authorizations to pass to the scanner; this, intersected with the user's allowed authorizations,
    * is checked against each entry's visibility to determine whether to return that entry to the user.
    */
  val authorizations : Authorizations

  /**
    * Applies these settings to a scanner.
    *
    * @param scanner scanner to configure.
    */
  protected final def apply(scanner : ScannerBase) : Unit = {
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
    *
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
    *
    * @param timeout The timeout for retries in the event of I/O failures; may be undefined/infinite to retry
    *                indefinitely.
    * @param batchTimeout The amount of time to wait for the batch buffer to fill before returning the
    *                     results; may be undefined/infinite to always wait for the buffer to completely fill.
    * @param classLoaderContext Optional classloader context to pass to the underlying scanner.
    * @param authorizations Scan authorizations to pass to the scanner; this, intersected with the user's
    *                       allowed authorizations, s checked against each entry's visibility to determine
    *                       whether to return that entry to the user.
    * @param batchSize The number of entries to attempt to return from the server at a time.
    * @param readAheadThreshold The number of batches of entries the scanner will return before triggering
    *                           readahead in the background.
    * @param isolationEnabled Whether to enable scan isolation.
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
    *
    * @param timeout The timeout for retries in the event of I/O failures; may be undefined/infinite to retry
    *                indefinitely.
    * @param batchTimeout The amount of time to wait for the batch buffer to fill before returning the
    *                     results; may be undefined/infinite to always wait for the buffer to completely fill.
    * @param classLoaderContext Optional classloader context to pass to the underlying scanner.
    * @param authorizations Scan authorizations to pass to the scanner; this, intersected with the user's
    *                       allowed authorizations, s checked against each entry's visibility to determine
    *                       whether to return that entry to the user.
    * @param numberOfQueryThreads Size of thread pool to use to query multiple tablet servers simultaneously.
    */
  final case class Batch(
    timeout: Duration = Duration.Undefined,
    batchTimeout: Duration = Duration.Undefined,
    classLoaderContext: Option[String] = None,
    authorizations: Authorizations = new Authorizations,
    numberOfQueryThreads : Int = 2
  ) extends ScannerSettings

}


