package cromwell

import akka.actor.{Actor, Props}
import cromwell.core.io._

object MockIoActor {
  def props = Props(new MockIoActor)
}

class MockIoActor extends Actor {
  override def receive = {
    case command: IoCopyCommand => sender() ! IoSuccess(command, ())
    case command: IoWriteCommand => sender() ! IoSuccess(command, ())
    case command: IoDeleteCommand => sender() ! IoSuccess(command, ())
    case command: IoSizeCommand => sender() ! IoSuccess(command, 100L)
    case command: IoContentAsStringCommand => sender() ! IoSuccess(command, "hello")
  }
}
