package cromwell.engine.io

import akka.actor.{Actor, Props}

object IoWorkers {
  def props(nb: Int) = Props(new IoWorkers(nb))
}

class IoWorkers(nb: Int) extends Actor {
  1 to nb map { i =>
    context.actorOf(IoWorkerActor.props(s"io-worker-$i-internal"), s"io-worker-$i")
  }
  
  override def receive: Receive = {
    case _ =>
  }
}
