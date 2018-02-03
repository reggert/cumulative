package io.github.reggert.cumulative.core

import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.hadoop.mapreduce.Job


/**
  * Interface for serializable factories for Accumulo [[Connector]] objects.
  * This is needed in order to guarantee that a connector can be obtained in a distributed context, e.g.,
  * Apache Spark.
  */
trait ConnectorProvider extends HadoopJobConfigurer  {
  /**
    * Returns an Accumulo [[Connector]] suitable for use in the local JVM.
    */
  def connector : Connector
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

  override def equals(other: Any): Boolean = other match {
    case that: PasswordTokenProvider =>
      password == that.password
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(password)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
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


  def canEqual(other: Any): Boolean = other.isInstanceOf[BasicClientConfigurationProvider]

  override def equals(other: Any): Boolean = other match {
    case that: BasicClientConfigurationProvider =>
      (that canEqual this) &&
        instanceName == that.instanceName &&
        connectString == that.connectString
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(instanceName, connectString)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
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


  def canEqual(other: Any): Boolean = other.isInstanceOf[ZookeeperConnectorProvider]


  override def equals(other: Any): Boolean = other match {
    case that: ZookeeperConnectorProvider =>
      (that canEqual this) &&
        clientConfigurationProvider == that.clientConfigurationProvider &&
        principal == that.principal &&
        authenticationTokenProvider == that.authenticationTokenProvider
    case _ => false
  }


  override def hashCode(): Int = {
    val state = Seq(clientConfigurationProvider, principal, authenticationTokenProvider)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}


//noinspection ScalaDeprecation
class MockConnectorProvider(
  val instanceName : String,
  val principal : String,
  val authenticationTokenProvider : AuthenticationTokenProvider
) extends ConnectorProvider {
  @transient lazy val instance = MockConnectorProvider.instanceByName(instanceName)
  @transient lazy val connector: Connector =
    instance.getConnector(principal, authenticationTokenProvider.authenticationToken)

  override def configure(configuration: Job): Unit = {
    AbstractInputFormat.setConnectorInfo(
      configuration,
      principal,
      authenticationTokenProvider.authenticationToken
    )
    AbstractInputFormat.setMockInstance(
      configuration,
      instanceName
    )
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[MockConnectorProvider]

  override def equals(other: Any): Boolean = other match {
    case that: MockConnectorProvider =>
      (that canEqual this) &&
        instanceName == that.instanceName &&
        principal == that.principal &&
        authenticationTokenProvider == that.authenticationTokenProvider
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(instanceName, principal, authenticationTokenProvider)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}


//noinspection ScalaDeprecation
object MockConnectorProvider {
  import scala.collection.concurrent
  private val instances = concurrent.TrieMap.empty[String, MockInstance]
  private def instanceByName(name : String) : MockInstance =
    instances.getOrElseUpdate(name, new MockInstance(name))
}
