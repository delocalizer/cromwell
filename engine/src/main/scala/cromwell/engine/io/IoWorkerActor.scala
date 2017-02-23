package cromwell.engine.io

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props}
import akka.pattern.{Backoff, BackoffSupervisor}
import com.google.cloud.storage.StorageException
import cromwell.core.io._

import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import scala.language.postfixOps

object IoWorkerActor {
  
  def props(name: String,
            minBackoff: FiniteDuration = 1 second,
            maxBackoff: FiniteDuration = 10 seconds,
            randomFactor: Double = 0.2D
           ) = {
    BackoffSupervisor.props(
      Backoff.onFailure(
        childProps = Props(new IoWorkerActor),
        childName = name,
        minBackoff = minBackoff,
        maxBackoff = maxBackoff,
        randomFactor = randomFactor
      ).withManualReset
        .withSupervisorStrategy(
          OneForOneStrategy() {
            case _: NotImplementedError => Escalate
            case other => Restart
          }
        )
    )
  }
}

class IoWorkerActor extends Actor with ActorLogging {
  
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    message foreach { self ! _ }
    super.preRestart(reason, message)
  }
  
  override def receive: Receive = {
    case command: IoCommand[_] => processCommand(command)
  }

  private def processCommand(command: IoCommand[_]): Unit = {
    def executeOperation = command match {
      case copyCommand: CopyCommand => copy(copyCommand) map copyCommand.success
      case writeCommand: WriteCommand => write(writeCommand) map writeCommand.success
      case deleteCommand: DeleteCommand => delete(deleteCommand) map deleteCommand.success
      case sizeCommand: SizeCommand => size(sizeCommand) map sizeCommand.success
      case readAsStringCommand: ReadAsStringCommand => readAsString(readAsStringCommand) map readAsStringCommand.success
      case _ => Failure(new NotImplementedError("Method not implemented"))
    }

    executeOperation match {
      case Success(Some(response)) =>
        context.parent ! BackoffSupervisor.Reset
        sender() ! response
      case Success(None) =>
        context.parent ! BackoffSupervisor.Reset
      case Failure(failure) if isRetryable(failure) =>
        log.error("Failed to execute IO operation")
        throw failure
    }
  }
  
  private def copy(copy: CopyCommand) = Try {
    copy.file.copyTo(copy.destination, copy.overwrite)
    ()
  }

  private def write(write: WriteCommand) = Try {
    write.file.write(write.content)
    ()
  }
  
  private def delete(delete: DeleteCommand) = Try {
    delete.file.delete(delete.swallowIOExceptions)
    ()
  }

  private def readAsString(read: ReadAsStringCommand) = Try {
    read.file.contentAsString
  }
  
  private def size(size: SizeCommand) = Try {
    size.file.size
  }

  /**
    * Returns true if the failure can be retried
    */
  protected def isRetryable(failure: Throwable): Boolean = failure match {
    case gcs: StorageException => gcs.retryable()
    case _ => false
  }
}
