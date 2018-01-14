package io.github.reggert.cumulative.core

import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}


/**
  * Interface for serializable factories for Accumulo [[Connector]] objects.
  * This is needed in order to guarantee that a connector can be obtained in a distributed context, e.g.,
  * Apache Spark.
  */
trait ConnectorFactory extends Serializable {
  /**
    * Constructs a new Accumulo [[Connector]].
    *
    * @return a configured connector for use in the local JVM.
    */
  def newConnector() : Connector
}


/**
  * Interface for serializable factories for Accumulo [[AuthenticationToken]] objects.
  * This is needed in order to guarantee that a token can be obtained in a distributed context, e.g.,
  * Apache Spark.
  */
trait AuthenticationTokenProvider extends Serializable {
  /**
    * Creates a new instance of [[AuthenticationToken]] to use in the local JVM.
    */
  def newAuthenticationToken() : AuthenticationToken
}


/**
  * Serializable provider of instances of [[PasswordToken]].
  *
  * @param password the password to use.
  */
final class PasswordTokenProvider(val password : String) extends AuthenticationTokenProvider {
  override def newAuthenticationToken(): AuthenticationToken = new PasswordToken(password)
}


/**
  * Creates Accumulo [[Connector]] objects that us Zookeeper.
  *
  * @param instanceName Accumulo instance name.
  * @param connectString Zookeeper connection string.
  * @param principal security principal to use for login (username).
  * @param authenticationTokenProvider security token to use for login.
  */
class ZookeeperConnectorFactory(
  val instanceName : String,
  val connectString : String,
  val principal : String,
  val authenticationTokenProvider : AuthenticationTokenProvider
) extends ConnectorFactory {
  override def newConnector(): Connector = {
    val zookeeperInstance = new ZooKeeperInstance(instanceName, connectString)
    zookeeperInstance.getConnector(principal, authenticationTokenProvider.newAuthenticationToken())
  }
}
