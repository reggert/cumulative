package io.github.reggert.cumulative.core

import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.hadoop.mapreduce.Job


/**
  * Interface for serializable factories for Accumulo [[Connector]] objects.
  * This is needed in order to guarantee that a connector can be obtained in a distributed context, e.g.,
  * Apache Spark.
  */
trait ConnectorProvider extends Serializable {
  /**
    * Returns an Accumulo [[Connector]] suitable for use in the local JVM.
    */
  def connector : Connector

  /**
    * Applies the connector settings to the specified Hadoop job configuration.
    *
    * @param configuration Hadoop job configuration to which to apply settings.
    */
  def configure(configuration : Job) : Unit
}


/**
  * Interface for serializable factories for Accumulo [[AuthenticationToken]] objects.
  * This is needed in order to guarantee that a token can be obtained in a distributed context, e.g.,
  * Apache Spark.
  */
trait AuthenticationTokenProvider extends Serializable {
  /**
    * Creates (if necessary) an instance of [[AuthenticationToken]] to use in the local JVM.
    */
  def authenticationToken : AuthenticationToken
}


/**
  * Serializable provider of instances of [[PasswordToken]].
  *
  * @param password the password to use.
  */
final class PasswordTokenProvider(val password : String) extends AuthenticationTokenProvider {
  @transient lazy val authenticationToken = new PasswordToken(password)
}


/**
  * Interface for serializable factories for Accumulo [[ClientConfiguration]] objects.
  * This is needed in order to guarantee that a token can be obtained in a distributed context, e.g.,
  * Apache Spark.
  */
trait ClientConfigurationProvider extends Serializable {
  /**
    * Creates (if necessary) an instance of [[ClientConfiguration]] to use in the local JVM.
    */
  def clientConfiguration : ClientConfiguration
}


/**
  * Creates an Accumulo [[ClientConfiguration]] providing basic access via Zookeeper.
  * This can be extended to provide additional configuration options by overriding `createClientConfiguration`
  * to modify the configuration it returns.
  *
  * @param instanceName Accumulo instance name.
  * @param connectString Zookeeper connection string.
  */
class BasicClientConfigurationProvider(
  val instanceName : String,
  val connectString : String
) extends ClientConfigurationProvider {
  protected def createClientConfiguration() : ClientConfiguration =
    ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(connectString)

  @transient final lazy val clientConfiguration = createClientConfiguration()
}


/**
  * Creates an Accumulo [[Connector]] that uses Zookeeper.
  *
  * The created connector is stored for reuse on subsequent calls within the same JVM, but will be re-created
  * if the provider is serialized and sent to another JVM.
  *
  * @param clientConfigurationProvider provider of an Accumulo [[ClientConfiguration]].
  * @param principal security principal to use for login (username).
  * @param authenticationTokenProvider security token to use for login.
  */
class ZookeeperConnectorProvider(
  val clientConfigurationProvider: ClientConfigurationProvider,
  val principal : String,
  val authenticationTokenProvider : AuthenticationTokenProvider
) extends ConnectorProvider {
  @transient lazy val instance = new ZooKeeperInstance(clientConfigurationProvider.clientConfiguration)
  @transient lazy val connector : Connector =
    instance.getConnector(principal, authenticationTokenProvider.authenticationToken)

  override def configure(configuration: Job): Unit = {
    AbstractInputFormat.setConnectorInfo(
      configuration,
      principal,
      authenticationTokenProvider.authenticationToken
    )
    AbstractInputFormat.setZooKeeperInstance(
      configuration,
      clientConfigurationProvider.clientConfiguration
    )
  }
}
