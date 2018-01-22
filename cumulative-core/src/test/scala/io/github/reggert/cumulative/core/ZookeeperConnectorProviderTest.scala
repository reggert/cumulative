package io.github.reggert.cumulative.core

import java.io._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.hadoop.mapreduce.Job
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._
import resource._


/**
  * Unit tests for [[ZookeeperConnectorProvider]].
  */
class ZookeeperConnectorProviderTest extends FunSuite with Matchers with BeforeAndAfterAll {
  val tempDir = Files.createTempDirectory(null)
  val rootPassword = "password"
  val accumuloCluster = new MiniAccumuloCluster(tempDir.toFile, rootPassword)
  val authenticationTokenProvider = new PasswordTokenProvider(rootPassword)
  val configurationProvider =
    new BasicClientConfigurationProvider(accumuloCluster.getInstanceName, accumuloCluster.getZooKeepers)
  val connectorProvider =
    new ZookeeperConnectorProvider(configurationProvider, "root", authenticationTokenProvider)


  override def beforeAll() : Unit = {
    accumuloCluster.start()
  }


  override def afterAll(): Unit = {
    accumuloCluster.stop()
    /*Files.walkFileTree(tempDir, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        super.visitFile(file, attrs)
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        super.postVisitDirectory(dir, exc)
      }
    })*/
  }



  test("ZookeeperConnectorProvider.connector") {
    val connector = connectorProvider.connector
    connector.securityOperations().listLocalUsers().asScala should not be empty
  }


  // ZookeeperConnectorProvider.configure needs to be tested by the tests for Scan,
  // since this method on its own doesn't configure enough to be meaningful.


  test("ZookeeperConnectorProvider serializable") {
    val serialized: Array[Byte] = {
      for {
        byteArrayOutputStream <- managed(new ByteArrayOutputStream)
        objectOutputStream <- managed(new ObjectOutputStream(byteArrayOutputStream))
      } yield {
        objectOutputStream.writeObject(connectorProvider)
        byteArrayOutputStream
      }
    }.acquireAndGet(_.toByteArray)
    val deserialized : ZookeeperConnectorProvider = {
      for {
        byteArrayInputStream <- managed(new ByteArrayInputStream(serialized))
        objectInputStream <- managed(new ObjectInputStream(byteArrayInputStream))
      } yield objectInputStream
    }.acquireAndGet(_.readObject().asInstanceOf[ZookeeperConnectorProvider])
    deserialized should equal (connectorProvider)
  }
}
