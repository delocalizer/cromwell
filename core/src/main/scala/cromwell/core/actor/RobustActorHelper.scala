package cromwell.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import cromwell.core.actor.RobustActorHelper._
import cromwell.core.actor.StreamIntegration._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

object RobustActorHelper {
  case class NoResponseTimeout(robustActorRequest: RobustActorMessage)
  case class RobustActorMessage(message: Any, to: ActorRef)
  private [actor] case class StreamClientTimers(backpressureTimer: Option[Cancellable], timeoutTimer: Cancellable, requestAttemptsCounter: Int) {
    def cancelTimers() = {
      backpressureTimer foreach { _.cancel() }
      timeoutTimer.cancel()
    }
  }
}

trait RobustActorHelper { this: Actor with ActorLogging =>
  private [actor] var requestsMap = Map.empty[RobustActorMessage, StreamClientTimers]
  private [actor] implicit val robuseActorHelperEc = context.dispatcher

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
      1
    )
    to ! message
    requestsMap = requestsMap updated (robustMessage, timers)
  }

  protected def robustReceive: Receive = {
    case Backpressure(request) => handleBackpressure(request)
    case NoResponseTimeout(robustActorMessage) => handleRequestLost(robustActorMessage)
  }

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
          timeoutTimer = newRequestLostTimer(robustMessage),
          1
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

        if (timers.requestAttemptsCounter < requestLostAttempts) {
          // Create a new requestLost timer and increment requestLostCounter
          val newTimers = StreamClientTimers(
            backpressureTimer = None,
            timeoutTimer = newRequestLostTimer(robustMessage),
            timers.requestAttemptsCounter + 1
          )
          // Update the internal map
          requestsMap = requestsMap updated(robustMessage, newTimers)
          // Resend the message
          robustMessage.to ! robustMessage.message
        } else {
          // If we reach requestLostAttempts, declare the service unreachable and remove from the map
          requestsMap = requestsMap - robustMessage
          onServiceUnreachable(robustMessage)
        }
      case None =>
        log.warning("Received a request lost message from a request that was never sent. Ignoring...")
    }
  }
}