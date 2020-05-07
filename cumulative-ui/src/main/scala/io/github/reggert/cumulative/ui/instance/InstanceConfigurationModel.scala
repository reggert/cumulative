package io.github.reggert.cumulative.ui.instance

import javax.swing.table.AbstractTableModel
import org.apache.accumulo.core.client.admin.InstanceOperations

import scala.collection.JavaConverters._
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.swing.{Dialog, Swing}
import scala.util.{Failure, Success}


/**
  * Data model for showing system configuration.
  *
  * @param instanceOperations service to retrieve/update system configuration.
  * @param executionContext context in which run background tasks.
  */
class InstanceConfigurationModel(instanceOperations: InstanceOperations)(implicit executionContext: ExecutionContext) extends AbstractTableModel {
  private var settings : IndexedSeq[(String, String)] = Vector.empty

  override def getRowCount: Int = settings.size

  override def getColumnCount: Int = 2

  override def getColumnName(columnIndex: Int): String = columnIndex match {
    case 0 => "Name"
    case 1 => "Value"
  }

  override def getColumnClass(columnIndex: Int): Class[_] = classOf[String]

  override def isCellEditable(rowIndex: Int, columnIndex: Int): Boolean = columnIndex match {
    case 0 => false
    case 1 => true
  }

  override def getValueAt(rowIndex: Int, columnIndex: Int): String = {
    val (name, value) = settings(rowIndex)
    columnIndex match {
      case 0 => name
      case 1 => value
    }
  }

  override def setValueAt(aValue: Any, rowIndex: Int, columnIndex: Int): Unit = {
    val (name, _) = settings(rowIndex)
    columnIndex match {
      case 1 =>
        val newValue = aValue.toString
        Future {
          blocking {instanceOperations.setProperty(name, newValue)}
        }.onComplete {
          case Success(_) => refresh()
          case Failure(e) =>
            e.printStackTrace(System.err)
            Swing.onEDT {
              Dialog.showMessage(
                title = "Error updating system configuration",
                message = e.getMessage,
                messageType = Dialog.Message.Error
              )
            }
        }
    }
  }


  def refresh() : Unit = {
    Future {
      blocking {
        instanceOperations.getSystemConfiguration
      }.asScala.toVector
    }.onComplete {
      case Success(newSettings) => Swing.onEDT {
        settings = newSettings
        fireTableDataChanged()
      }
      case Failure(e) =>
        e.printStackTrace(System.err)
        Swing.onEDT {
          Dialog.showMessage(
            title = "Error retrieving system configuration",
            message = e.getMessage,
            messageType = Dialog.Message.Error
          )
        }
    }
  }
}
