package io.github.reggert.cumulative.core

import java.io._
import java.util.UUID

import org.scalatest.{FunSuite, Matchers}
import resource._

import scala.collection.JavaConverters._


/**
  * Unit tests for [[MockConnectorProvider]].
  */
class MockConnectorProviderTest extends FunSuite with Matchers {
  val rootPassword = ""
  val authenticationTokenProvider = new PasswordTokenProvider(rootPassword)
  val instanceName: String = UUID.randomUUID().toString
  val connectorProvider =
    new MockConnectorProvider(instanceName, "root", authenticationTokenProvider)


  test("MockConnectorProvider.connector") {
    val connector = connectorProvider.connector
    connector.securityOperations().listLocalUsers().asScala should not be empty
  }


  // MockConnectorProvider.configure needs to be tested by the tests for Scan,
  // since this method on its own doesn't configure enough to be meaningful.


  test("MockConnectorProvider serializable") {
    val serialized: Array[Byte] = {
      for {
        byteArrayOutputStream <- managed(new ByteArrayOutputStream)
        objectOutputStream <- managed(new ObjectOutputStream(byteArrayOutputStream))
      } yield {
        objectOutputStream.writeObject(connectorProvider)
        byteArrayOutputStream
      }
    }.acquireAndGet(_.toByteArray)
    val deserialized : MockConnectorProvider = {
      for {
        byteArrayInputStream <- managed(new ByteArrayInputStream(serialized))
        objectInputStream <- managed(new ObjectInputStream(byteArrayInputStream))
      } yield objectInputStream
    }.acquireAndGet(_.readObject().asInstanceOf[MockConnectorProvider])
    deserialized should equal (connectorProvider)
  }
}
