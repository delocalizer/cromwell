package cromwell.backend.callcaching

import akka.actor.{Actor, ActorLogging, ActorRef}
import cromwell.backend.BackendInitializationData
import cromwell.core.JobKey
import cromwell.core.actor.AsyncIo
import cromwell.core.callcaching.HashKey
import wdl4s.values.WdlFile

object AsyncFileHashingActor {

  /**
    * @param files files is an iterable (and not a List) on purpose, so it can support lazy collection implementation and reduce memory footprint
    */
  case class MultiFileHashRequest(jobKey: JobKey, hashKey: HashKey, files: Iterable[WdlFile], initializationData: Option[BackendInitializationData])
}

case class AsyncFileHashingActor(ioActor: ActorRef) extends Actor with ActorLogging with AsyncIo {
  override def receive: Receive = {
    case _ =>
  }
}
