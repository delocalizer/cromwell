package cromwell.core

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import akka.pattern.pipe
import akka.stream.QueueOfferResult
import akka.stream.QueueOfferResult._
import akka.stream.scaladsl.SourceQueueWithComplete
import cromwell.core.StreamClientHelper.StreamClientTimers
import cromwell.core.StreamIntegration._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object StreamIntegration {
  trait StreamContext {
    def replyTo: ActorRef
    def request: Any
  }
  case class EnqueueResponse(response: QueueOfferResult, request: StreamContext)
  case class Backpressure(request: Any)
  case class FailedToEnqueue(failure: Throwable, request: StreamContext)
  case class EnqueuingException(throwable: Throwable) extends RuntimeException(throwable)
}

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

object StreamClientHelper {
  private case class StreamClientTimers(backoffTimer: Cancellable, timeoutTimer: Cancellable) {
    def cancelTimers() = {
      backoffTimer.cancel()
      timeoutTimer.cancel()
    }
  }
}

trait StreamClientHelper { this: Actor with ActorLogging =>
  private var requestsMap = Map.empty[Any, StreamClientTimers]
  
  protected def lostTimeout: FiniteDuration
  protected def backpressureTimeout: FiniteDuration
  protected def actorReceive: Receive
  
  protected def onServiceUnreachable(): Unit

  private def streamReceive: Receive = {
    case Backpressure(request) => 
  }

  // Schedule a DockerNoResponseTimeout message to be sent to self
  private final def newRequestLostTimer = akka.pattern.after(lostTimeout)
  // Schedule a DockerHashRequest to be sent to the docker hashing actor
  private final def newBackPressureTimer(dockerHashRequest: DockerHashRequest) = context.system.scheduler.scheduleOnce(backpressureWaitTime, dockerHashingActor, dockerHashRequest)(ec, self)
  
  private def handleBackpressure(request: Any) = {
    requestsMap.get(request) foreach { _.cancelTimers() }
  }
}