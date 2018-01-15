package io.github.reggert.cumulative.core.mutate

import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.{BatchWriter, ConditionalWriter, Connector, MultiTableBatchWriter}

import scala.util.Try


/**
  * Interface for classes that write to a single table.
  * This is intended as a serializable facade to Accumulo's [[BatchWriter]].
  */
trait TableWriter extends Serializable {
  /**
    * Writes the specified collection of mutations to Accumulo, flushes the buffers, and cleans up any
    * underlying resources.
    *
    * @param mutations mutations to write.
    */
  def apply(mutations : TraversableOnce[RowMutation]) : Unit
}


/**
  * Interface for classes that write to multiple tables.
  * This is intended as a serializable facade to Accumulo's [[MultiTableBatchWriter]].
  */
trait MultiTableWriter extends Serializable {
  /**
    * Writes the specified collection of mutations to Accumulo, flushes the buffers, and cleans up any
    * underlying resources.
    *
    * @param mutationsByTable mutations to write, keyed by table name.
    */
  def apply(mutationsByTable : TraversableOnce[(TableName, RowMutation)]) : Unit
}


trait ConditionalTableWriter extends Serializable {
  /**
    * Conditionally writes the specified collection of mutations to Accumulo, flushes the buffers, and cleans
    * up any underlying resources.
    *
    * @param mutations mutations to write.
    * @return the results of attempting to write the mutations.
    */
  def apply(mutations : Traversable[ConditionalRowMutation]) : List[ConditionalTableWriter.Result]
}


object TableWriter {
  /**
    * Constructs a `TableWriter` for the specified table.
    * @param tableName         the table to which to write.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param writerSettings settings to use.
    * @return a new `TableWriter`.
    */
  def apply(tableName: TableName)(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings = WriterSettings()
  ) : TableWriter = new DefaultImplementation(tableName)

  /**
    * Default implementation of `TableWriter`.
    * @param tableName table to which to write.
    * @param connectorProvider provider of Accumulo [[Connector]].
    * @param writerSettings settings to use.
    */
  private final class DefaultImplementation(val tableName : TableName)(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings
  ) extends TableWriter {
    override def apply(mutations: TraversableOnce[RowMutation]): Unit = {
      val batchWriter = connectorProvider.connector.createBatchWriter(
        tableName.toString,
        writerSettings.toBatchWriterConfig
      )
      try {
        mutations.map(_.toAccumuloMutation).foreach(batchWriter.addMutation)
      }
      finally {
        batchWriter.close()
      }
    }
  }
}


object MultiTableWriter {
  /**
    * Constructs a `MultiTableWriter` for the specified table.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param writerSettings settings to use.
    * @return a new `MultiTableWriter`.
    */
  def apply()(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings = WriterSettings()
  ) : MultiTableWriter = new DefaultImplementation

  /**
    * Default implementation of `MultiTableWriter`.
    * @param connectorProvider provider of Accumulo [[Connector]].
    * @param writerSettings settings to use.
    */
  private final class DefaultImplementation(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings
  ) extends MultiTableWriter {
    override def apply(mutationsByTable: TraversableOnce[(TableName, RowMutation)]): Unit = {
      val multiTableBatchWriter =
        connectorProvider.connector.createMultiTableBatchWriter(writerSettings.toBatchWriterConfig)
      try {
        mutationsByTable.foreach {
          case (tableName, rowMutation) =>
            val batchWriter = multiTableBatchWriter.getBatchWriter(tableName.toString)
            batchWriter.addMutation(rowMutation.toAccumuloMutation)
        }
      }
      finally {
        multiTableBatchWriter.close()
      }
    }
  }
}


object ConditionalTableWriter {

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

  /**
    * Constructs a `ConditionalTableWriter` for the specified table.
    * @param tableName         the table to which to write.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param writerSettings settings to use.
    * @return a new `ConditionalTableWriter`.
    */
  def apply(tableName: TableName)(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings = WriterSettings()
  ) : ConditionalTableWriter = new DefaultImplementation(tableName)

  /**
    * Default implementation of `ConditionalTableWriter`.
    * @param tableName table to which to write.
    * @param connectorProvider provider of Accumulo [[Connector]].
    * @param writerSettings settings to use.
    */
  private final class DefaultImplementation(val tableName : TableName)(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings
  ) extends ConditionalTableWriter {
    override def apply(mutations: Traversable[ConditionalRowMutation]): List[Result] = {
      val conditionalWriter = connectorProvider.connector.createConditionalWriter(
        tableName.toString,
        writerSettings.toConditionalWriterConfig
      )
      try {
        mutations.map { mutation =>
          val result = conditionalWriter.write(mutation.toAccumuloMutation)
          Result(mutation, result.getTabletServer, Try(result.getStatus))
        }.toList
      }
      finally {
        conditionalWriter.close()
      }
    }
  }
}
