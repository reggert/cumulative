package scala.swing.event

import scala.swing.InternalFrame

abstract class InternalFrameEvent(override val source : InternalFrame) extends UIEvent

final case class InternalFrameOpened(override val source : InternalFrame) extends InternalFrameEvent(source)

final case class InternalFrameClosing(override val source : InternalFrame) extends InternalFrameEvent(source)

final case class InternalFrameClosed(override val source : InternalFrame) extends InternalFrameEvent(source)

final case class InternalFrameIconified(override val source : InternalFrame) extends InternalFrameEvent(source)

final case class InternalFrameDeiconified(override val source : InternalFrame) extends InternalFrameEvent(source)

final case class InternalFrameActivated(override val source : InternalFrame) extends InternalFrameEvent(source)

final case class InternalFrameDeactivated(override val source : InternalFrame) extends InternalFrameEvent(source)

