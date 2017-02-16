package cromwell.engine.io

import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestActorRef}
import cromwell.core.TestKitSuite
import cromwell.core.io.messages.{CopyCommandMessage, IoSuccess}
import cromwell.core.path.{DefaultPathBuilder, Path}
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class IoActorSpec extends TestKitSuite with FlatSpecLike with Matchers with ImplicitSender {
  behavior of "IoActor"
  
  implicit val actorSystem = system
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer = ActorMaterializer()
  
  it should "copy files" in {
    val testActor = TestActorRef(new IoActor(1, new IoFlow(1)))
    
    val src = DefaultPathBuilder.createTempFile()
    val dst: Path = src.parent.resolve(src.name + "-dst")
    
    val copyCommand = CopyCommandMessage(src, dst)
    
    testActor ! copyCommand
    expectMsgPF(5 seconds) {
      case response: IoSuccess[_] => response.command.isInstanceOf[CopyCommandMessage] shouldBe true
      case r => println(r)
    }
    
    dst.exists shouldBe true
    src.delete()
    dst.delete()
  }
}
