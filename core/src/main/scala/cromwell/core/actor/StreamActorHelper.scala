package cromwell.core.actor

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import akka.stream.QueueOfferResult
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.SourceQueueWithComplete
import cromwell.core.actor.StreamIntegration._

import scala.concurrent.{ExecutionContext, Future}

trait StreamActorHelper { this: Actor with ActorLogging =>

  implicit val ec: ExecutionContext = context.system.dispatcher

  protected def actorReceive: Receive

  override def receive = streamReceive.orElse(actorReceive)

  def sendToStream[T <: StreamContext](commandContext: T, stream: SourceQueueWithComplete[T]) = {
    val enqueue = stream offer commandContext map { result =>
      EnqueueResponse(result, commandContext)
    } recoverWith {
      case t => Future.successful(FailedToEnqueue(t, commandContext))
    }

    pipe(enqueue) to self
    ()
  }

  private def streamReceive: Receive = {
    case EnqueueResponse(Enqueued, commandContext) => // All good !
    case EnqueueResponse(Dropped, commandContext) => commandContext.replyTo ! Backpressure(commandContext.request)
    // In any of the below case, the stream is in a state where it will not be able to receive new elements.
    // This means something blew off so we can just restart the actor to re-instantiate a new stream
    case EnqueueResponse(QueueClosed, commandContext) =>
      val exception = new RuntimeException(s"Failed to enqueue request ${commandContext.request}. Queue was closed")
      logAndRestart(exception, commandContext)
    case EnqueueResponse(QueueOfferResult.Failure(failure), commandContext) =>
      logAndRestart(failure, commandContext)
    case FailedToEnqueue(throwable, commandContext) =>
      logAndRestart(throwable, commandContext)
  }

  private def logAndRestart(throwable: Throwable, commandContext: StreamContext) = {
    commandContext.replyTo ! FailedToEnqueue(throwable, commandContext)
    log.warning("Failed to process request: {}", throwable.getMessage)
    // Throw the exception that will be caught by supervisor and restart the actor
    throw EnqueuingException(throwable)
  }
}
