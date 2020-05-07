package io.github.reggert.cumulative.ui.instance

import org.apache.accumulo.core.client.admin.InstanceOperations

import scala.concurrent.ExecutionContext
import scala.swing.event.Key
import scala.swing.{Component, DesktopPane, InternalFrame, Menu, MenuItem}

class InstanceController(instanceOperations: InstanceOperations)(implicit executionContext: ExecutionContext) {
  val configurationModel = new InstanceConfigurationModel(instanceOperations)
  val configurationView = new InstanceConfigurationView(configurationModel)
  def views : Seq[InternalFrame] = Seq(configurationView)

  val menu: Menu = new Menu("Instance") {
    contents += new MenuItem("Configuration") {
      mnemonic = Key.C
      reactions += {
        case _ =>
          configurationView.show()
          configurationView.moveToFront()
          configurationView.select()
      }
    }
  }

  def dispose() : Unit = views foreach (_.dispose())
}
