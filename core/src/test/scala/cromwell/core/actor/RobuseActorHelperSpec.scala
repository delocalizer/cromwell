package cromwell.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import cromwell.core.TestKitSuite
import cromwell.core.actor.RobustActorHelper.RobustActorMessage
import cromwell.core.actor.StreamIntegration.Backpressure
import org.scalatest.{FlatSpecLike, Matchers}

class RobuseActorHelperSpec extends TestKitSuite with FlatSpecLike with Matchers with ImplicitSender {
  behavior of "BackpressureActorHelper"
  
  it should "create appropriate timers when sendRobustMessage is called" in {
    val remoteActor = TestProbe()
    val delegateActor = TestProbe()
    val testActor = TestActorRef(new TestActor(delegateActor.ref))
    val messageToSend = TestActor.TestMessage("hello")
    testActor.underlyingActor.sendMessage(messageToSend, remoteActor.ref)
    
    val maybeTimers1 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers1 shouldBe a[Some[_]]
    maybeTimers1.get.backpressureTimer shouldBe None
    maybeTimers1.get.timeoutTimer.isCancelled shouldBe false
    maybeTimers1.get.requestLostCounter shouldBe 0
    
    remoteActor.expectMsg(messageToSend)
    remoteActor.reply(Backpressure(messageToSend))
    
    val maybeTimers2 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers2 shouldBe a[Some[_]]
    maybeTimers2.get.backpressureTimer shouldBe a[Some[_]]
    maybeTimers2.get.backpressureTimer.get.isCancelled shouldBe false
    maybeTimers2.get.timeoutTimer.isCancelled shouldBe false
    maybeTimers2.get.requestLostCounter shouldBe 0
  }
  
  private [actor] object TestActor {
    case class TestMessage(v: String)
    case object ServiceUnreachable
  }
  private class TestActor(delegateTo: ActorRef) extends Actor with ActorLogging with RobustActorHelper {
    var messageSent: Option[Any] = None
    
    override def receive: Receive = {
      case message => 
        responseReceived(messageSent.get)
        delegateTo ! message
    }
    
    def sendMessage(message: Any, to: ActorRef) = {
      messageSent = Option(message)
      sendRobustMessage(message, to)
    }
    override protected def onServiceUnreachable(robustActorMessage: RobustActorMessage): Unit = {
      delegateTo ! TestActor.ServiceUnreachable
    }
  }
}
