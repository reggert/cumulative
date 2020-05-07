package io.github.reggert.cumulative.ui.instance

import scala.swing.event.InternalFrameOpened
import scala.swing.{Action, BoxPanel, Button, Dimension, InternalFrame, Orientation, Table, ToolBar}

class InstanceConfigurationView(val model : InstanceConfigurationModel) extends InternalFrame { view =>
  title = "System Configuration"
  contents = new BoxPanel(Orientation.Vertical) {
    contents += new ToolBar {
      contents += new Button(new Action("Refresh") {
        override def apply(): Unit = model.refresh()
      })
    }
    contents += new Table {
      model = view.model
    }
  }
  reactions += {
    case InternalFrameOpened(_) => model.refresh()
  }
  size = new Dimension(640, 480)
  closable = true
  maximizable = true

  override def closeOperation(): Unit = hide()
}
