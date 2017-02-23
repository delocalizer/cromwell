package cromwell.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import cromwell.core.TestKitSuite
import cromwell.core.actor.RobustActorHelper.RobustActorMessage
import cromwell.core.actor.StreamIntegration.Backpressure
import org.scalatest.{FlatSpecLike, Matchers}
import scala.concurrent.duration._
import scala.language.postfixOps

class RobuseActorHelperSpec extends TestKitSuite with FlatSpecLike with Matchers with ImplicitSender {
  behavior of "BackpressureActorHelper"
  
  it should "handle Backpressure responses" in {
    val remoteActor = TestProbe()
    val delegateActor = TestProbe()
    
    val margin = 1 second
    val backpressureTimeout = 1 second
    val lostTimeout = 3 seconds
    val maxAttempts = 2
    val testActor = TestActorRef(new TestActor(delegateActor.ref, backpressureTimeout, lostTimeout, maxAttempts))
    
    val messageToSend = TestActor.TestMessage("hello")
    
    //send message
    testActor.underlyingActor.sendMessage(messageToSend, remoteActor.ref)
    
    val maybeTimers1 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers1 shouldBe a[Some[_]]
    maybeTimers1.get.backpressureTimer shouldBe None
    maybeTimers1.get.timeoutTimer.isCancelled shouldBe false
    maybeTimers1.get.requestAttemptsCounter shouldBe 1
    
    // remote actor receives message
    remoteActor.expectMsg(messageToSend)
    
    // remote actor sends a backpressure message
    remoteActor.reply(Backpressure(messageToSend))
    
    // remote actore expectes request again after backpressureTimeout
    remoteActor.expectMsg(backpressureTimeout + margin, messageToSend)
    val maybeTimers2 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers2 shouldBe a[Some[_]]
    maybeTimers2.get.requestAttemptsCounter shouldBe 1
    
    // remote actor replies
    remoteActor.reply("world")
    
    // delegate actor receives response
    delegateActor.expectMsg("world")
    
    // timers are cancelled
    remoteActor.expectNoMsg()
  }

  it should "handle a successful response" in {
    val remoteActor = TestProbe()
    val delegateActor = TestProbe()

    val backpressureTimeout = 1 second
    val lostTimeout = 3 seconds
    val maxAttempts = 2
    val testActor = TestActorRef(new TestActor(delegateActor.ref, backpressureTimeout, lostTimeout, maxAttempts))

    val messageToSend = TestActor.TestMessage("hello")

    // send message
    testActor.underlyingActor.sendMessage(messageToSend, remoteActor.ref)

    val maybeTimers1 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers1 shouldBe a[Some[_]]
    maybeTimers1.get.backpressureTimer shouldBe None
    maybeTimers1.get.timeoutTimer.isCancelled shouldBe false
    maybeTimers1.get.requestAttemptsCounter shouldBe 1

    // remote actor receives message
    remoteActor.expectMsg(messageToSend)
    
    // remote actor replies
    remoteActor.reply("world")
    
    // delegate receives response
    delegateActor.expectMsg("world")
    
    // timers are cancelled
    remoteActor.expectNoMsg()
  }

  it should "retry if remote service doesn't respond and finally call onServiceUnreachable after the max number of attempt has been reached" in {
    val remoteActor = TestProbe()
    val delegateActor = TestProbe()

    val margin = 1 second
    // Make this very high on purpose to make sure the right timeout (lostTimeout) is used when no answer is received
    val backpressureTimeout = 50 seconds
    val lostTimeout = 1 second
    val maxAttempts = 3
    val testActor = TestActorRef(new TestActor(delegateActor.ref, backpressureTimeout, lostTimeout, maxAttempts))

    val messageToSend = TestActor.TestMessage("hello")

    // send a message - Attempt #1
    testActor.underlyingActor.sendMessage(messageToSend, remoteActor.ref)
    
    // remote expects the message
    remoteActor.expectMsg(messageToSend)
    
    // remote doesn't answer
    
    // remote receives the message again after ~lostTimeout  - Attempt #2
    remoteActor.expectMsg(lostTimeout + margin, messageToSend)

    // counter is incremented
    val maybeTimers1 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers1 shouldBe a[Some[_]]
    maybeTimers1.get.requestAttemptsCounter shouldBe 2

    // remote doesn't answer
    
    // remote receives the message again after ~lostTimeout  - Attempt #3
    remoteActor.expectMsg(lostTimeout + margin, messageToSend)

    // remote doesn't answer
    
    // The next lost timeout timer will make the counter reach max attempt
    // The timers should be canceled and the entry removed from the map
    Thread.sleep((lostTimeout + margin).toMillis)
    
    // Max number of attempts reached, entry should be removed from map and timers cancelled
    val maybeTimers2 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers2 shouldBe None

    // delegate should receive the ServiceUnreachable dummy message
    delegateActor.expectMsg(TestActor.ServiceUnreachable)
    
    // timers should be cancelled
    remoteActor.expectNoMsg()
  }

  it should "reset attempt counter if it gets a response" in {
    val remoteActor = TestProbe()
    val delegateActor = TestProbe()

    val margin = 1 second
    val backpressureTimeout = 1 second
    val lostTimeout = 3 second
    val maxAttempts = 2
    val testActor = TestActorRef(new TestActor(delegateActor.ref, backpressureTimeout, lostTimeout, maxAttempts))

    val messageToSend = TestActor.TestMessage("hello")

    // Send a message
    testActor.underlyingActor.sendMessage(messageToSend, remoteActor.ref)
    
    // remote receives the message but doesn't reply
    remoteActor.expectMsg(messageToSend)

    // remote receives the message again after ~lostTimeout
    remoteActor.expectMsg(lostTimeout + margin, messageToSend)
    
    // lost counter should be incremented
    val maybeTimers1 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers1 shouldBe a[Some[_]]
    maybeTimers1.get.requestAttemptsCounter shouldBe 2

    // remote actor replies with backpressure
    remoteActor.reply(Backpressure(messageToSend))
    // counter should be back to 0
    val maybeTimers2 = testActor.underlyingActor.requestsMap.get(RobustActorMessage(messageToSend, remoteActor.ref))
    maybeTimers2 shouldBe a[Some[_]]
    maybeTimers2.get.requestAttemptsCounter shouldBe 1
    
    // remote expects message again after ~backpressureTimeout
    remoteActor.expectMsg(backpressureTimeout + margin, messageToSend)
    
    // remote replies with response
    remoteActor.reply("world")
    
    // delegate receives response
    delegateActor.expectMsg("world")
    
    // remote should not receive anything else (timers have been cancelled)
    remoteActor.expectNoMsg()
  }
  
  private [actor] object TestActor {
    case class TestMessage(v: String)
    case object ServiceUnreachable
  }
  private class TestActor(delegateTo: ActorRef, override val backpressureTimeout: FiniteDuration, override val lostTimeout: FiniteDuration, override val requestLostAttempts: Int) extends Actor with ActorLogging with RobustActorHelper {
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
