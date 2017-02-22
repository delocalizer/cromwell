package cromwell.engine.io

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import cromwell.core.StreamActorHelper
import cromwell.core.StreamIntegration.StreamContext
import cromwell.core.io.IoCommand
import cromwell.core.io.messages.IoRetry
import cromwell.engine.io.IoActor._

import scala.concurrent.ExecutionContext
import scala.language.existentials

final class IoActor(queueSize: Int, ioFlow: IoFlow)(implicit materializer: ActorMaterializer) extends Actor with ActorLogging with StreamActorHelper {
  private val ec: ExecutionContext = context.dispatcher
  
  private val source = Source.queue[IoCommandContext](queueSize, OverflowStrategy.dropNew)
  private val flow = ioFlow.flow
  private val sink = Sink.foreach[(Option[Any], IoCommandContext)] {
    case (Some(response: IoRetry[_]), commandContext) => 
      context.system.scheduler.scheduleOnce(response.waitTime, self, commandContext.request.withNextBackoff)(ec, commandContext.replyTo)
      ()
    case (Some(response), commandContext) => commandContext.replyTo ! response
    case _ =>
  }
  
  private val stream: SourceQueueWithComplete[IoCommandContext] = source
    .via(flow)
    .to(sink)
    .run()

  override def actorReceive: Receive = {
    case command: IoCommand[_] => 
      val replyTo = sender()
      val commandContext= IoCommandContext(command, replyTo)
      sendToStream(commandContext, stream)
  }
}

object IoActor {
  case class IoCommandContext(request: IoCommand[_], replyTo: ActorRef) extends StreamContext
  def props(queueSize: Int, flow: IoFlow)(implicit materializer: ActorMaterializer) = Props(new IoActor(queueSize, flow))
}
