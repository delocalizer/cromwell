package cromwell.engine.io

import akka.actor.{ActorRef, Scheduler}
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import com.google.cloud.storage.StorageException
import cromwell.core.io._
import cromwell.engine.io.IoActor.IoCommandContext
import cromwell.engine.io.IoFlow.IoResult

import scala.concurrent.{ExecutionContext, Future}

object IoFlow {
  type IoResult = (IoAck[_], ActorRef)
}

class IoFlow(parallelism: Int, scheduler: Scheduler, throttleOptions: Option[Throttle] = None)(implicit ec: ExecutionContext) {
  private val processCommand: IoCommandContext[_] => Future[IoResult] = commandContext => {
    val operationResult: Future[IoAck[_]] = commandContext.request match {
      case singleCommand: IoSingleCommand[_] => handleSingleCommand(singleCommand)
      case batchCommand: IoBatchCommand[_] => throw new NotImplementedError("Batch not implemented yet") // handleBatchCommand(batchCommand)
    }
    
    operationResult map { (_, commandContext.replyTo) } recoverWith recoverFailure(commandContext)
  }
  
  private def handleSingleCommand(ioSingleCommand: IoSingleCommand[_]) = {
    ioSingleCommand match {
      case copyCommand: IoCopyCommand => copy(copyCommand) map copyCommand.success
      case writeCommand: IoWriteCommand => write(writeCommand) map writeCommand.success
      case deleteCommand: IoDeleteCommand => delete(deleteCommand) map deleteCommand.success
      case sizeCommand: IoSizeCommand => size(sizeCommand) map sizeCommand.success
      case readAsStringCommand: IoContentAsStringCommand => readAsString(readAsStringCommand) map readAsStringCommand.success
      case _ => Future.failed(new NotImplementedError("Method not implemented"))
    }
  }
  
//  private def handleBatchCommand(batchedCommand: IoBatchCommand[_]) = Future {
//    batchedCommand.execute()
//    val (success, failure) = batchedCommand.get partition(_.isSuccess)
//    if (failure.isEmpty) success map { _.get }
//    else {
//      
//    }
//  }
  
  private val processFlow = Flow[IoCommandContext[_]].mapAsyncUnordered[IoResult](parallelism)(processCommand)
  
  val flow = throttleOptions match {
    case Some(options) => 
      val throttleFlow = Flow[IoCommandContext[_]].throttle(options.elements, options.per, options.maximumBurst, ThrottleMode.Shaping)
      throttleFlow.via(processFlow)
    case None => processFlow
  }
  
  private def copy(copy: IoCopyCommand) = Future {
    copy.file.copyTo(copy.destination, copy.overwrite)
    ()
  }

  private def write(write: IoWriteCommand) = Future {
    write.file.write(write.content)
    ()
  }
  
  private def delete(delete: IoDeleteCommand) = Future {
    delete.file.delete(delete.swallowIOExceptions)
    ()
  }

  private def readAsString(read: IoContentAsStringCommand) = Future {
    read.file.contentAsString
  }
  
  private def size(size: IoSizeCommand) = Future {
    size.file.size
  }

  /**
    * Returns true if the failure can be retried
    */
  protected def isRetryable(failure: Throwable): Boolean = failure match {
    case gcs: StorageException => gcs.retryable()
    case _ => false
  }
  
  private def recoverFailure(commandContext: IoCommandContext[_]): PartialFunction[Throwable, Future[IoResult]] = {
    case failure if isRetryable(failure) => recoverRetryableFailure(commandContext, failure)
    case failure => Future.successful(commandContext.fail(failure))
  }
  
  private def recoverRetryableFailure(commandContext: IoCommandContext[_], failure: Throwable): Future[IoResult] = {
    commandContext.request.timeBeforeNextTry match {
      case Some(waitTime) =>
        commandContext.request match {
          case singleCommand: IoSingleCommand[_] =>
            akka.pattern.after(waitTime, scheduler)(processCommand(commandContext.withNextBackoff))
          case batchCommand: IoBatchCommand[_] =>
            akka.pattern.after(waitTime, scheduler)(processCommand(commandContext.withNextBackoff))
        }
      case None => Future.successful(commandContext.fail(failure))
    }
  }
  
}
