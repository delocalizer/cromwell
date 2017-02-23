package cromwell.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import cromwell.core.actor.RobustActorHelper.RobustActorMessage
import cromwell.core.io._
import cromwell.core.path.BetterFileMethods.OpenOptions
import cromwell.core.path.Path

import scala.concurrent.{Future, Promise}

trait AsyncIo extends RobustActorHelper { this: Actor with ActorLogging =>
  
  def ioActor: ActorRef
  
  var promiseMap = Map.empty[IoCommand[_], Promise[_]]
  
  private final def responseReceive: Receive = {
    case ack: IoAck[Any] @ unchecked =>
      responseReceived(ack.command)
      promiseMap.get(ack.command) match {
        case Some(promise) => 
          // This is pretty scary and clearly not typesafe. However, the sendIoCommand method ensures that the generic types for any key/value pair added to the map match.
          // Moreover, the generic type of an IoAck matches the type of its command by design.
          // Which means as long as only the sendIoCommand method is used to add new entries, the types are virtually guaranteed to match.
          promise.asInstanceOf[Promise[Any]].complete(ack.toTry)
          promiseMap = promiseMap - ack.command
        case None => 
          log.warning(s"Cannot find a promise to complete for Io command ${ack.command}")
      }
  }
  
  protected final def ioReceive = responseReceive orElse robustReceive
  
  def contentAsStringAsync(path: Path): Future[String] = {
    val promise = Promise[String]
    sendIoCommand(IoContentAsStringCommand(path), promise)
    promise.future
  }
  
  def writeAsync(path: Path, content: String, options: OpenOptions): Future[Unit] = {
    val promise = Promise[Unit]
    sendIoCommand(IoWriteCommand(path, content, options), promise)
    promise.future
  }

  def sizeAsync(path: Path): Future[Long] = {
    val promise = Promise[Long]
    sendIoCommand(IoSizeCommand(path), promise)
    promise.future
  }

  def deleteAsync(path: Path, swallowIoExceptions: Boolean = false): Future[Unit] = {
    val promise = Promise[Unit]
    sendIoCommand(IoDeleteCommand(path, swallowIoExceptions), promise)
    promise.future
  }

  def copyAsync(src: Path, dest: Path, overwrite: Boolean = true): Future[Unit] = {
    val promise = Promise[Unit]
    sendIoCommand(IoCopyCommand(src, dest, overwrite), promise)
    promise.future
  }

  private def sendIoCommand[T](command: IoCommand[T], promise: Promise[T]) = {
    promiseMap = promiseMap updated (command, promise)
    sendRobustMessage(command, ioActor)
  }

  override protected def onServiceUnreachable(robustActorMessage: RobustActorMessage): Unit = {
    ()
  }
}
