package cromwell.core.io.messages

import cromwell.core.io.IoCommand

import scala.concurrent.duration.FiniteDuration

/**
  * Generic trait for values returned after a command is executed. Can be Success or Failure.
  *
  * @tparam T type of the returned value if success
  */
sealed trait IoMessageAck[T] {
  /**
    * Original command
    */
  def command: IoCommand[T]
}

case class IoSuccess[T](command: IoCommand[T], result: T) extends IoMessageAck[T]
case class IoRetry[T](command: IoCommand[T], waitTime: FiniteDuration) extends IoMessageAck[T]
case class IoFailure[T](command: IoCommand[T], failure: Throwable) extends IoMessageAck[T]