package io.github.reggert.cumulative.ui

import java.io.File

import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, KerberosToken, PasswordToken}
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import scopt.OParser

import scala.swing.Frame
import scala.util.Try

/**
  * Main entry point for Cumulative UI application.
  */
object CumulativeUI extends App {
  OParser.parse(CumulativeUIConfig.parser, args, CumulativeUIConfig()) foreach { config =>
    val frame = new Frame {
      title = s"CumulativeUI: ${config.instance.getInstanceName}"
    }
  }
}


final case class CumulativeUIConfig(
  user: String = System.getProperty("user.name"),
  password: Option[String] = None,
  useKerberos: Boolean = false,
  kerberosKeytab: Option[File] = None,
  zookeeperConnectString: Option[String] = None,
  instanceName: Option[String] = None,
  configFile: Option[File] = None
) {
  lazy val authenticationToken: AuthenticationToken = {
    if (useKerberos) {
      kerberosKeytab.map(keytab => new KerberosToken(user, keytab)).getOrElse(new KerberosToken(user))
    }
    else {
      password.map(pwd => new PasswordToken(pwd))
        .getOrElse {
          new PasswordToken(System.console().readPassword("Enter password: "))
        }
    }
  }

  lazy val clientConfiguration: ClientConfiguration = {
    val c = configFile.map(ClientConfiguration.fromFile).getOrElse(ClientConfiguration.loadDefault())
    zookeeperConnectString.foreach(c.withZkHosts)
    instanceName.foreach(c.withInstance)
    c
  }

  lazy val instance: ZooKeeperInstance =
    new ZooKeeperInstance(clientConfiguration)

  lazy val connector: Connector =
    instance.getConnector(user, authenticationToken)
}


object CumulativeUIConfig {
  private val builder = scopt.OParser.builder[CumulativeUIConfig]

  val parser: OParser[Unit, CumulativeUIConfig] = {
    import builder._
    programName("CumulativeUI")
    head("CumulativeUI")
    opt[String]('u', "user")
      .action((value, options) => options.copy(user = value))
      .text("username to use with Accumulo")
    opt[String]('p', "password")
      .action((value, options) => options.copy(password = Some(value), useKerberos = false))
      .text("password to use with Accumulo")
    opt[Option[File]]('K', "kerberos")
      .action((value, options) => options.copy(useKerberos = true, kerberosKeytab = value))
      .valueName("keytab-file")
      .text("enables Kerberos authentication and optionally specifies a keytab file")
    opt[String]('c', "connect")
      .action((value, options) => options.copy(zookeeperConnectString = Some(value)))
      .text("Zookeeper connection string")
      .valueName("server:port,...")
    opt[String]('i', "instance")
      .action((value, options) => options.copy(instanceName = Some(value)))
      .text("Accumulo instance name")
    opt[File]('C', "config-file")
      .action((value, options) => options.copy(configFile = Some(value)))
      .text("Accumulo config file")
    checkConfig { config =>
      Try(config.connector.securityOperations().authenticateUser(config.user, config.authenticationToken))
        .map(result => if (result) success else failure("Authentication failed"))
        .get
    }
  }
}
