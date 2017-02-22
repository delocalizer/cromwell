package cromwell.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.testkit.{TestActorRef, TestProbe}
import cromwell.core.TestKitSuite
import org.scalatest.{FlatSpecLike, Matchers}

class RobuseActorHelperSpec extends TestKitSuite with FlatSpecLike with Matchers {
  behavior of "BackpressureActorHelper"
  
  it should "create appropriate timers when sendRobustMessage is called" in {
    val remoteActor = TestProbe()
    val delegateActor = TestProbe()
    val testActor = TestActorRef(new TestActor(delegateActor.ref))
    
    
  }
  
  private object TestActor {
    case class TestMessage(v: String)
  }
  private class TestActor(delegateTo: ActorRef) extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => delegateTo ! message
    }
  }
}
