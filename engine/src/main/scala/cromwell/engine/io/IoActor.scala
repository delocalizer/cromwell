package cromwell.engine.io

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import cromwell.core.actor.StreamActorHelper
import cromwell.core.actor.StreamIntegration.StreamContext
import cromwell.core.io.{IoCommand, IoSingleCommand}
import cromwell.engine.io.IoActor._
import cromwell.engine.io.IoFlow.IoResult

final class IoActor(queueSize: Int, ioFlow: IoFlow)(implicit materializer: ActorMaterializer) extends Actor with ActorLogging with StreamActorHelper {
  private val source = Source.queue[IoCommandContext[_]](queueSize, OverflowStrategy.dropNew)
  private val flow = ioFlow.flow
  private val sink = Sink.foreach[IoResult] {
    case (response, replyTo) => replyTo ! response 
  }
  
  private val stream: SourceQueueWithComplete[IoCommandContext[_]] = source.via(flow).to(sink).run()

  override def actorReceive: Receive = {
    case command: IoSingleCommand[_] => 
      val replyTo = sender()
      val commandContext= IoCommandContext(command, replyTo)
      sendToStream(commandContext, stream)
  }
}

object IoActor {
  case class IoCommandContext[T](request: IoCommand[T], replyTo: ActorRef) extends StreamContext {
    def withNextBackoff = copy(request = request.withNextBackoff)
    def isRetryable = request.timeBeforeNextTry.isDefined
    def fail(failure: Throwable) = (request.fail(failure), replyTo)
    def success(value: T) = (request.success(value), replyTo)
  }
  
  def props(queueSize: Int, flow: IoFlow)(implicit materializer: ActorMaterializer) = Props(new IoActor(queueSize, flow))
}
