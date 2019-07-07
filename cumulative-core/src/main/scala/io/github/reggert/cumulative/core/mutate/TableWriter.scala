package io.github.reggert.cumulative.core.mutate

import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.{BatchWriter, Connector}
import resource._


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
  def apply(mutations : TraversableOnce[RowMutation]) : Unit =
    session.foreach {s => mutations.foreach(s.write)}

  /**
    * Obtains a resource-managed session for writing mutations.
    *
    * @return a writer session as a managed resource.
    */
  def session : ManagedResource[TableWriter.Session]
}


object TableWriter {
  /**
    * Constructs a `TableWriter` for the specified table.
    *
    * @param tableName         the table to which to write.
    * @param connectorProvider provider of the Accumulo [[Connector]].
    * @param writerSettings settings to use.
    * @return a new `TableWriter`.
    */
  def apply(tableName: TableName)(implicit
    connectorProvider : ConnectorProvider,
    writerSettings: WriterSettings = WriterSettings()
  ) : TableWriter = new TableWriter {
    override def session: ManagedResource[Session] =
      managed(
        connectorProvider.connector.createBatchWriter(
          tableName.toString,
          writerSettings.toBatchWriterConfig
        )
      ).map(Session(_))
  }

  /**
    * Buffered session to write to a single Accumulo table.
    */
  trait Session {
    /**
      * Buffers the specified mutation to write to the tablet server.
      *
      * @param mutation mutation to write.
      */
    def write(mutation : RowMutation) : Unit

    /**
      * Flushes all buffered mutations to Accumulo.
      */
    def flush() : Unit
  }

  object Session {
    /**
      * Creates a session that wraps a [[BatchWriter]].
      *
      * @param batchWriter the [[BatchWriter]] to wrap.
      */
    def apply(batchWriter: BatchWriter): Session = new Session {
      override def write(mutation: RowMutation): Unit = batchWriter.addMutation(mutation.toAccumuloMutation)
      override def flush(): Unit = batchWriter.flush()
    }
  }
}






