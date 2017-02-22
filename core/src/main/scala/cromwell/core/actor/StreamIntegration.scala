package cromwell.core.actor

import akka.actor.ActorRef
import akka.stream.QueueOfferResult

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
