package io.github.reggert.cumulative.core.scan

import java.util

import io.github.reggert.cumulative.core.data._
import io.github.reggert.cumulative.core.{ConnectorProvider, TableName}
import org.apache.accumulo.core.client.{BatchScanner, Connector, Scanner}
import org.apache.accumulo.core.data.{Range => AccumuloRange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._


/**
  * Unit tests for [[Scan]].
  */
class ScanTest extends FunSuite with Matchers with MockFactory {

  test("SimpleScan") {
    val connector = mock[Connector]
    implicit val connectorProvider: ConnectorProvider = stub[ConnectorProvider]
    (connectorProvider.connector _).when().returns(connector)
    implicit val scannerSettings: ScannerSettings.Simple = ScannerSettings.Simple(
      batchSize = 100,
      readAheadThreshold = 2L,
      authorizations = new Authorizations("vis1")
    )
    val columns = Set(
      ColumnSelector(family = ColumnFamily("family1"), qualifier = None),
      ColumnSelector(family = ColumnFamily("family2"), qualifier = Some(ColumnQualifier("qualifier2")))
    )
    val range = ScanRange.MaximumRow(ScanRange.Inclusive(RowIdentifier("999")))
    val entries = List(
      Entry(
        EntryKey(
          RowIdentifier("123"),
          ColumnIdentifier(ColumnFamily("family1"), ColumnQualifier("qualfier1")),
          timestamp = Timestamp.Specified(12345678L)
        ),
        EntryValue("abc")
      ),
      Entry(
        EntryKey(
          RowIdentifier("456"),
          ColumnIdentifier(ColumnFamily("family2"), ColumnQualifier("qualfier2")),
          timestamp = Timestamp.Specified(12345999L)
        ),
        EntryValue("def")
      )
    )
    val scanner = mock[Scanner]
    scanner.setBatchSize _ expects scannerSettings.batchSize
    scanner.setRange _ expects range.toAccumuloRange
    scanner.setReadaheadThreshold _ expects scannerSettings.readAheadThreshold
    scanner.fetchColumnFamily _ expects new Text("family1")
    (scanner.fetchColumn(_ : Text, _ : Text)) expects (new Text("family2"), new Text("qualifier2"))
    (scanner.iterator _).expects().returns(entries.view.map(_.toAccumuloEntry).asJava.iterator())
    (scanner.close _).expects()

    val tableName = TableName("TestTable")
    (connector.createScanner _).expects(tableName.toString, scannerSettings.authorizations).returns(scanner)

    val scan = Scan.Simple(tableName, range, columns = columns)
    scan.results.acquireAndGet(_.toList) should equal (entries)
  }



  test("BatchScan") {
    val connector = mock[Connector]
    implicit val connectorProvider: ConnectorProvider = stub[ConnectorProvider]
    (connectorProvider.connector _).when().returns(connector)
    implicit val scannerSettings: ScannerSettings.Batch = ScannerSettings.Batch(
      authorizations = new Authorizations("vis1"),
      numberOfQueryThreads = 3
    )
    val columns = Set(
      ColumnSelector(family = ColumnFamily("family1"), qualifier = None),
      ColumnSelector(family = ColumnFamily("family2"), qualifier = Some(ColumnQualifier("qualifier2")))
    )
    val ranges = Set(ScanRange.ExactRow(RowIdentifier("123")), ScanRange.ExactRow(RowIdentifier("456")))
    val entries = List(
      Entry(
        EntryKey(
          RowIdentifier("123"),
          ColumnIdentifier(ColumnFamily("family1"), ColumnQualifier("qualfier1")),
          timestamp = Timestamp.Specified(12345678L)
        ),
        EntryValue("abc")
      ),
      Entry(
        EntryKey(
          RowIdentifier("456"),
          ColumnIdentifier(ColumnFamily("family2"), ColumnQualifier("qualfier2")),
          timestamp = Timestamp.Specified(12345999L)
        ),
        EntryValue("def")
      )
    )
    val scanner = mock[BatchScanner]
    scanner.setRanges _ expects where { arg: util.Collection[AccumuloRange] =>
      arg.asScala.toSet == ranges.map(_.toAccumuloRange)
    }
    scanner.fetchColumnFamily _ expects new Text("family1")
    (scanner.fetchColumn(_ : Text, _ : Text)) expects (new Text("family2"), new Text("qualifier2"))
    (scanner.iterator _).expects().returns(entries.view.map(_.toAccumuloEntry).asJava.iterator())
    (scanner.close _).expects()

    val tableName = TableName("TestTable")
    (connector.createBatchScanner _).expects(
      tableName.toString,
      scannerSettings.authorizations,
      scannerSettings.numberOfQueryThreads
    ).returns(scanner)

    val scan = Scan.Batch(tableName, ranges, columns = columns)
    scan.results.acquireAndGet(_.toList) should equal (entries)
  }
}
