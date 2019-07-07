package io.github.reggert.cumulative.core.mutate

import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.MultiTableBatchWriter
import resource._


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
  def apply(mutationsByTable : TraversableOnce[(TableName, RowMutation)]) : Unit =
    session.foreach {s =>
      mutationsByTable.foreach { case (tableName, mutation) =>
          s(tableName).write(mutation)
      }
    }

  /**
    * Obtains a resource-managed session for writing mutations.
    *
    * @return a writer session as a managed resource.
    */
  def session : ManagedResource[MultiTableWriter.Session]
}


object MultiTableWriter {

  /**
    * Buffered session to write to multiple Accumulo tables.
    */
  trait Session {
    /**
      * Obtains the sub-session for the specified table.
      *
      * @param tableName name of the table to which to write.
      * @return a single-table session.
      */
    def apply(tableName : TableName) : TableWriter.Session

    /**
      * Flushes all buffered writes to Accumulo.
      */
    def flush() : Unit
  }

  /**
    * Constructs a `MultiTableWriter` for the specified table.
    *
    * @param connectorProvider provider of the Accumulo [[org.apache.accumulo.core.client.Connector]].
    * @param writerSettings    settings to use.
    * @return a new `MultiTableWriter`.
    */
  def apply()(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings = WriterSettings()
  ) : MultiTableWriter = new MultiTableWriter {
    override def session: ManagedResource[MultiTableWriter.Session] =
      managed(
        connectorProvider.connector.createMultiTableBatchWriter(writerSettings.toBatchWriterConfig)
      ).map { multiTableBatchWriter =>
        new Session {
          override def apply(tableName: TableName): TableWriter.Session =
            TableWriter.Session(multiTableBatchWriter.getBatchWriter(tableName.toString))

          override def flush(): Unit = multiTableBatchWriter.flush()
        }
      }
  }
}