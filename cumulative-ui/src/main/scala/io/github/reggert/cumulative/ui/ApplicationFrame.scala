package io.github.reggert.cumulative.ui

import io.github.reggert.cumulative.ui.instance.InstanceController

import scala.concurrent.ExecutionContext
import scala.swing.event.Key
import scala.swing.{DesktopPane, Dimension, MainFrame, MenuBar, MenuItem}


class ApplicationFrame(config : CumulativeUIConfig)(implicit executionContext: ExecutionContext) extends MainFrame { main =>
  private val instanceController = new InstanceController(config.connector.instanceOperations())
  private val desktop = new DesktopPane {
    contents ++= instanceController.views
  }
  title = s"CumulativeUI: ${config.instance.getInstanceName}"
  contents = desktop
  menuBar = new MenuBar {
    contents += instanceController.menu
  }
  instanceController.menu.contents += new MenuItem("Exit") {
    mnemonic = Key.X
    reactions += {
      case _ =>
        instanceController.dispose()
        main.dispose()
    }
  }
  size = new Dimension(640, 480)
  centerOnScreen()
  open()
}
