package cromwell.core

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import akka.pattern.pipe
import akka.stream.QueueOfferResult
import akka.stream.QueueOfferResult._
import akka.stream.scaladsl.SourceQueueWithComplete
import cromwell.core.BackpressuredActorHelper.{NoResponseTimeout, RobustActorMessage, StreamClientTimers}
import cromwell.core.StreamIntegration._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

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

object BackpressuredActorHelper {
  case class NoResponseTimeout(robustActorRequest: RobustActorMessage)
  case class RobustActorMessage(message: Any, to: ActorRef)
  private case class StreamClientTimers(backpressureTimer: Option[Cancellable], timeoutTimer: Cancellable, requestLostCounter: Int) {
    def cancelTimers() = {
      backpressureTimer foreach { _.cancel() }
      timeoutTimer.cancel()
    }
  }
}

trait BackpressuredActorHelper { this: Actor with ActorLogging =>
  private var requestsMap = Map.empty[RobustActorMessage, StreamClientTimers]
  
  protected def requestLostAttempts: Int = 2
  protected def lostTimeout: FiniteDuration = 20 seconds
  protected def backpressureTimeout: FiniteDuration = 5 seconds
  
  protected def onServiceUnreachable(robustActorMessage: RobustActorMessage): Unit

  /**
    * Don't forget to call this method when a response is received
    * so timers can be cancelled !
    */
  protected final def responseReceived(originalMessage: Any) = {
    findEntryFor(originalMessage) match {
      case Some((robustMessage, timers)) => 
        timers.cancelTimers()
        requestsMap = requestsMap - robustMessage
      case None =>
    }
  }

  /**
    * Sends message to "to" in a way that handles lost requests and backpressure responses.
    */
  protected final def sendRobustMessage(message: Any, to: ActorRef) = {
    val robustMessage = RobustActorMessage(message, to)
    val timers = StreamClientTimers(
      backpressureTimer = None,
      timeoutTimer = newRequestLostTimer(robustMessage),
      0
    )
    to ! message
    requestsMap = requestsMap updated (robustMessage, timers)
  }

  private def robustReceive: Receive = {
    case Backpressure(request) => handleBackpressure(request)
    case NoResponseTimeout(robustActorMessage) => handleRequestLost(robustActorMessage)
  }

  context.become(robustReceive.orElse(receive))

  /* Sends a message to self after lostTimeout. 
   * This covers the case where the request has been dropped by the receiver
   * for whatever reason
   */
  private final def newRequestLostTimer(robustActorMessage: RobustActorMessage) = {
    context.system.scheduler.scheduleOnce(lostTimeout, self, NoResponseTimeout(robustActorMessage))
  }

  /* Schedule the message to be resent after backpressureTimeout.
   * This covers the case where the receiver is overloaded and sends back a backpressure message.
   */
  private final def newBackPressureTimer(robustActorMessage: RobustActorMessage) = {
    context.system.scheduler.scheduleOnce(backpressureTimeout, robustActorMessage.to, robustActorMessage.message)
  }
  
  /*
   * Try to find an entry for this message in the map.
   */
  private def findEntryFor(request: Any) = {
    requestsMap.collectFirst {
      case (robustMessage @ RobustActorMessage(message, _), timers) if message == request => (robustMessage, timers)
    }
  }
  
  private def handleBackpressure(request: Any) = {
    findEntryFor(request) match {
      case Some((robustMessage, timers)) => 
        timers.cancelTimers()
        // Create new timers, including an exponential backoff timer
        val newTimers = timers.copy(
          backpressureTimer = Option(newBackPressureTimer(robustMessage)),
          timeoutTimer = newRequestLostTimer(robustMessage)
        )
        // Update the internal map
        requestsMap = requestsMap updated (robustMessage, newTimers)
      case None =>
        log.warning("Received a backpressure message from a request that was never sent. Ignoring...")
    }
  }
  
  private def handleRequestLost(robustMessage: RobustActorMessage) = {
    requestsMap.get(robustMessage) match {
      case Some(timers) =>
        timers.cancelTimers()
        
        if (timers.requestLostCounter < requestLostAttempts) {
          // Create a new requestLost timer and increment requestLostCounter
          val newTimers = StreamClientTimers(
            backpressureTimer = None,
            timeoutTimer = newRequestLostTimer(robustMessage),
            timers.requestLostCounter + 1
          )
          // Update the internal map
          requestsMap = requestsMap updated(robustMessage, newTimers)
          // Resend the message
          robustMessage.to ! robustMessage.message
        } else {
          // If we reach requestLostAttempts, just declare the service unreachable
          onServiceUnreachable(robustMessage)
        }
      case None =>
        log.warning("Received a request lost message from a request that was never sent. Ignoring...")
    }
  }
}