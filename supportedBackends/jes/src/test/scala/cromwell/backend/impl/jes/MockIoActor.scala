package cromwell.backend.impl.jes

import akka.actor.{Actor, Props}
import cromwell.core.io.messages._

object MockIoActor {
  def props = Props(new MockIoActor)
}

class MockIoActor extends Actor {
  override def receive = {
    case command: CopyCommandMessage => sender() ! IoSuccess(command, ())
    case command: WriteCommandMessage => sender() ! IoSuccess(command, ())
    case command: DeleteCommandMessage => sender() ! IoSuccess(command, ())
    case command: SizeCommandMessage => sender() ! IoSuccess(command, 100L)
    case command: ReadAsStringCommandMessage => sender() ! IoSuccess(command, "hello")
  }
}
