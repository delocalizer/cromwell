package cromwell.engine.io

import akka.actor.{Actor, ActorRef}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import cromwell.core.io.IoCommand
import cromwell.core.io.messages.IoRetry
import cromwell.engine.io.IoActor.IoCommandContext

import scala.concurrent.ExecutionContext
import scala.language.existentials

object IoActor {
  case class IoCommandContext(command: IoCommand[_], replyTo: ActorRef)
}

class IoActor(implicit materializer: ActorMaterializer, ec: ExecutionContext) extends Actor {
  val source = Source.queue[IoCommandContext](200, OverflowStrategy.dropNew)
  val flow = new IoFlow().build
  val sink = Sink.foreach[(Option[Any], IoCommandContext)] {
    case (Some(response: IoRetry[_]), commandContext) => 
      context.system.scheduler.scheduleOnce(response.waitTime, self, commandContext.command.withNextBackoff)
      ()
    case (Some(response), commandContext) => commandContext.replyTo ! response
    case _ =>
  }
    
  val stream = source
    .via(flow)
    .to(sink)
    .run()
  
  override def receive: Receive = {
    case command: IoCommand[_] => 
      val replyTo = sender()
      val commandContext= IoCommandContext(command, replyTo)
      stream offer commandContext
      ()
  }
}
