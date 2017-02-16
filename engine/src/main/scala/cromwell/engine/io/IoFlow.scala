package cromwell.engine.io

import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import com.google.cloud.storage.StorageException
import cromwell.core.io._
import cromwell.engine.io.IoActor.IoCommandContext

import scala.concurrent.{ExecutionContext, Future}

class IoFlow(parallelism: Int, throttleOptions: Option[Throttle] = None)(implicit ec: ExecutionContext) {
  
  private val processCommand: IoCommandContext => Future[(Option[Any], IoCommandContext)] = commandContext => {
    val operationResult = commandContext.request match {
      case copyCommand: CopyCommand => copy(copyCommand) map copyCommand.success
      case writeCommand: WriteCommand => write(writeCommand) map writeCommand.success
      case deleteCommand: DeleteCommand => delete(deleteCommand) map deleteCommand.success
      case sizeCommand: SizeCommand => size(sizeCommand) map sizeCommand.success
      case readAsStringCommand: ReadAsStringCommand => readAsString(readAsStringCommand) map readAsStringCommand.success
      case _ => Future.failed(new NotImplementedError("Method not implemented"))
    }
    
    operationResult map { (_, commandContext) } recoverWith recoverFailure(commandContext)
  }
  
  private val processFlow = Flow[IoCommandContext].mapAsyncUnordered[(Option[Any], IoCommandContext)](parallelism)(processCommand)
  
  val flow = throttleOptions match {
    case Some(options) => 
      val throttleFlow = Flow[IoCommandContext].throttle(options.elements, options.per, options.maximumBurst, ThrottleMode.Shaping)
      throttleFlow.via(processFlow)
    case None => processFlow
  }
  
  private def copy(copy: CopyCommand) = Future {
    copy.file.copyTo(copy.destination, copy.overwrite)
    ()
  }

  private def write(write: WriteCommand) = Future {
    write.file.write(write.content)
    ()
  }
  
  private def delete(delete: DeleteCommand) = Future {
    delete.file.delete(delete.swallowIOExceptions)
    ()
  }

  private def readAsString(read: ReadAsStringCommand) = Future {
    read.file.contentAsString
  }
  
  private def size(size: SizeCommand) = Future {
    size.file.size
  }

  /**
    * Returns true if the failure can be retried
    */
  protected def isRetryable(failure: Throwable): Boolean = failure match {
    case gcs: StorageException => gcs.retryable()
    case _ => false
  }
  
  /**
    * Utility method to wrap a failure into a IoFailure or IoRetry
    */
  private def recoverFailure(commandContext: IoCommandContext): PartialFunction[Throwable, Future[(Option[Any], IoCommandContext)]] = {
    case failure if isRetryable(failure) && commandContext.request.timeBeforeNextTry.isDefined => Future.successful(
      (
        commandContext.request.retry(commandContext.request.timeBeforeNextTry.get),
        commandContext
      )
    )
    case failure => Future.successful(
      (
        commandContext.request.fail(failure),
        commandContext
      )
    )
  }
}
