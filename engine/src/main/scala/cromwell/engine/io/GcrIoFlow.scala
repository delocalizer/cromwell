package cromwell.engine.io

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Partition}
import com.google.cloud.storage.StorageException
import cromwell.core.io.messages.{DeleteCommandMessage, ReadAsStringCommandMessage, WriteCommandMessage}
import cromwell.engine.io.IoActor.IoCommandContext
import cromwell.filesystems.gcs.GcsPath

import scala.concurrent.ExecutionContext

class GcrIoFlow(implicit ec: ExecutionContext) extends IoFlow {
  override def build: Flow[IoCommandContext, (Option[Any], IoCommandContext), NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    
    val partition = builder.add(partition)
    
    FlowShape
  }
  
  private val paritionBatchable = Partition[IoCommandContext](2, { commandContext =>
    commandContext.command match {
      case ReadAsStringCommandMessage(path: GcsPath, _) => 0
      case WriteCommandMessage(path: GcsPath, _, _, _) => 0
      case DeleteCommandMessage(path: GcsPath, _, _) => 0
      case _ => 1
    }
  })
  
  override protected def isRetryable(failure: Throwable): Boolean = failure match {
    case gcs: StorageException => gcs.retryable()
    case _ => false
  }
}
