package cromwell.engine.io

import akka.NotUsed
import akka.stream.scaladsl.Flow
import better.files.File
import better.files.File._
import com.google.cloud.storage.StorageException
import cromwell.core.io.CopyCommand
import cromwell.engine.io.IoActor.IoCommandContext

import scala.concurrent.{ExecutionContext, Future}

class IoFlow(implicit ec: ExecutionContext) {
  def build: Flow[IoCommandContext, (Option[Any], IoCommandContext), NotUsed] = {
    Flow[IoCommandContext].mapAsyncUnordered[(Option[Any], IoCommandContext)](10)(processCommand)
  }

  protected final val processCommand: IoCommandContext => Future[(Option[Any], IoCommandContext)] = commandContext => {
    val operationResult = commandContext.command match {
      case copyCommand: CopyCommand => copy(copyCommand)
      case _ => Future.failed(new NotImplementedError("Method not implemented"))
    }
    
    operationResult map { result => mapSuccess(commandContext, result) } recoverWith recoverFailure(commandContext)
  }
  
  protected def isRetryable(failure: Throwable): Boolean = failure match {
    case gcs: StorageException => gcs.retryable()
    case _ => false
  }
  
  private def copy(copy: CopyCommand): Future[Option[Any]] = Future {
    File(copy.source).copyTo(copy.destination, copy.overwrite)
  } map { _ => copy.success(()) }


  /**
    * Utility method to make a pair of the value and context
    */
  private def mapSuccess(commandContext: IoCommandContext, value: Option[Any]) = (value, commandContext)

  /**
    * Utility method to wrap a failure into a IoFailure
    */
  private def recoverFailure(commandContext: IoCommandContext): PartialFunction[Throwable, Future[(Option[Any], IoCommandContext)]] = {
    case failure if isRetryable(failure) && commandContext.command.timeBeforeNextTry.isDefined => Future.successful(
      (
        commandContext.command.retry(commandContext.command.timeBeforeNextTry.get),
        commandContext
      )
    )
    case failure => Future.successful(
      (
        commandContext.command.fail(failure),
        commandContext
      )
    )
  }
}
