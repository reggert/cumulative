package io.github.reggert.cumulative.core.mutate

import io.github.reggert.cumulative.core.{ConnectorProvider, TableName, mutate}
import org.apache.accumulo.core.client.ConditionalWriter

import scala.collection.JavaConverters._
import scala.util.Try
import resource._


/**
  * Interface for classes that conditionally write to a single table.
  */
trait ConditionalTableWriter extends Serializable {
  /**
    * Conditionally writes the specified collection of mutations to Accumulo, waits for the results, and cleans
    * up any underlying resources.
    *
    * @param mutations conditional mutations to write.
    * @return the results of attempting to write the mutations.
    */
  def apply(mutations : TraversableOnce[ConditionalRowMutation]) : List[ConditionalTableWriter.Result] =
    session.acquireAndGet(_.write(mutations).toList)

  /**
    * Conditionally writes the specified mutation to Accumulo, waits for the result, and cleans up any underlying
    * resources.
    *
    * @param mutation conditional mutation to write.
    * @return the result of attempting to write the mutation.
    */
  def apply(mutation : ConditionalRowMutation) : ConditionalTableWriter.Result =
    session.acquireAndGet(_.write(mutation))

  /**
    * Obtains a resource-managed session for writing mutations.
    *
    * @return a writer session as a managed resource.
    */
  def session : ManagedResource[mutate.ConditionalTableWriter.Session]
}


object ConditionalTableWriter {
  trait Session {
    /**
      * Writes a conditional mutation to Accumulo and returns the result.
      *
      * @param mutation the conditional mutation to apply.
      * @return result of applying the conditional mutation.
      */
    def write(mutation: ConditionalRowMutation) : Result

    /**
      * Writes multiple conditional mutations (possibly in parallel) and returns the results.
      *
      * @param mutations collection of mutations to apply.
      * @return an iterator over the results, in the same order as the mutations.
      */
    def write(mutations: TraversableOnce[ConditionalRowMutation]) : TraversableOnce[Result]
  }

  object Session {
    /**
      * Creates a session for the specified [[ConditionalWriter]].
      *
      * @param conditionalWriter the `ConditionalWriter` to wrap.
      * @return a session.
      */
    def apply(conditionalWriter: ConditionalWriter) : Session =
      new Session {
        override def write(mutation: ConditionalRowMutation): Result =
          Result(mutation, conditionalWriter.write(mutation.toAccumuloMutation))

        override def write(mutations: TraversableOnce[ConditionalRowMutation]): TraversableOnce[Result] = {
          // We need to memoize the mutations so we can link them to the results.
          val mutationsStream = mutations.toStream
          val accumuloMutations = mutationsStream.toIterator.map(_.toAccumuloMutation)
          val accumuloResults = conditionalWriter.write(accumuloMutations.asJava).asScala.toStream
          mutationsStream.zip(accumuloResults).map {case (mutation, accumuloResult) =>
            Result(mutation, accumuloResult)
          }.toIterator
        }
      }
  }

  /**
    * Result returned from a conditional write.
    *
    * @param mutation the conditional mutation that was attempted.
    * @param server the tablet server it affected.
    * @param status the status of the mutation, or an exception if the status could not be obtained.
    */
  final case class Result(
    mutation : ConditionalRowMutation,
    server : String,
    status : Try[ConditionalWriter.Status]
  )

  object Result {
    /**
      * Creates a `Result` object from a [[ConditionalWriter.Result]].
      *
      * @param mutation the [[ConditionalRowMutation]] to which the result applies.
      * @param accumuloResult the raw Accumulo result.
      * @return a `Result` object.
      */
    def apply(mutation : ConditionalRowMutation, accumuloResult: ConditionalWriter.Result) : Result =
      Result(mutation, accumuloResult.getTabletServer, Try(accumuloResult.getStatus))
  }

  /**
    * Constructs a `ConditionalTableWriter` for the specified table.
    *
    * @param tableName         the table to which to write.
    * @param connectorProvider provider of the Accumulo [[org.apache.accumulo.core.client.Connector]].
    * @param writerSettings    settings to use.
    * @return a new `ConditionalTableWriter`.
    */
  def apply(tableName: TableName)(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings = WriterSettings()
  ) : ConditionalTableWriter =
    new ConditionalTableWriter {
      override def session: ManagedResource[Session] = managed(
        connectorProvider.connector.createConditionalWriter(
          tableName.toString,
          writerSettings.toConditionalWriterConfig
        )
      ).map(Session(_))
    }
}