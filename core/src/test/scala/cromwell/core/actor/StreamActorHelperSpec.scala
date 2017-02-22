package cromwell.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.QueueOfferResult.QueueClosed
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.testkit.{ImplicitSender, TestActorRef}
import cromwell.core.TestKitSuite
import cromwell.core.actor.StreamIntegration._
import cromwell.core.actor.TestStreamActor.{TestStreamActorCommand, TestStreamActorContext}
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps

class StreamActorHelperSpec extends TestKitSuite with FlatSpecLike with Matchers with ImplicitSender {
  behavior of "StreamActorHelper"
  
  implicit val materializer = ActorMaterializer()

  it should "catch EnqueueResponse message" in {
    val actor = TestActorRef(Props(new TestStreamActor(1)))
    val command = new TestStreamActorCommand
    actor ! command
    expectNoMsg(1 second)
    system stop actor
  }

  it should "throw an exception when receiving QueueClosed" in {
    val actor = TestActorRef(new TestStreamActor(1))
    val command = new TestStreamActorCommand
    val commandContext = TestStreamActorContext(command, self)
    
    actor.underlyingActor.stream.complete()
    
    intercept[EnqueuingException] { actor.receive(EnqueueResponse(QueueClosed, commandContext)) }
    system stop actor
  }

  it should "reply with a failure when trying to enqueue to a completed stream" in {
    val actor = TestActorRef(new TestStreamActor(1))
    val command = new TestStreamActorCommand

    actor.underlyingActor.stream.complete()
    actor ! command
    expectMsgClass(classOf[FailedToEnqueue])

    system stop actor
  }

  it should "throw an exception when receiving QueueOfferResult.Failure" in {
    val actor = TestActorRef(new TestStreamActor(1))
    val command = new TestStreamActorCommand
    val commandContext = TestStreamActorContext(command, self)
    val exception = new Exception("Failed to enqueue - part of test flow")

    actor.underlyingActor.stream.complete()

    intercept[EnqueuingException] { actor.receive(EnqueueResponse(QueueOfferResult.Failure(exception), commandContext)) }
    system stop actor
  }

  it should "reply with a failure when trying to enqueue to a failed stream" in {
    val actor = TestActorRef(new TestStreamActor(1))
    val command = new TestStreamActorCommand
    val exception = new Exception("Stream failed")

    actor.underlyingActor.stream.fail(exception)
    actor ! command
    expectMsgClass(classOf[FailedToEnqueue])

    system stop actor
  }

  it should "throw an exception and reply with failure when receiving FailedToEnqueue" in {
    val actor = TestActorRef(new TestStreamActor(1))
    val command = new TestStreamActorCommand
    val commandContext = TestStreamActorContext(command, self)
    val exception = new Exception("Failed to enqueue - part of test flow")

    actor.underlyingActor.stream.complete()

    intercept[EnqueuingException] { actor.receive(FailedToEnqueue(exception, commandContext)) }
    expectMsgClass(classOf[FailedToEnqueue])
    system stop actor
  }

  it should "send a backpressure message when messages are dropped by the queue" in {
    val actor = TestActorRef(new TestStreamActor(1))
    val command = new TestStreamActorCommand
    val commandContext = TestStreamActorContext(command, self)

    actor ! EnqueueResponse(QueueOfferResult.Dropped, commandContext)
    
    expectMsgClass(classOf[Backpressure])
    system stop actor
  }
}


private object TestStreamActor {
  class TestStreamActorCommand
  case class TestStreamActorContext(request: TestStreamActorCommand, replyTo: ActorRef) extends StreamContext
}

private class TestStreamActor(queueSize: Int)(implicit materializer: ActorMaterializer) extends Actor with ActorLogging with StreamActorHelper {
  val stream = Source.queue[TestStreamActorContext](queueSize, OverflowStrategy.dropNew).to(Sink.ignore).run()

  override protected def actorReceive: Receive = {
    case command: TestStreamActorCommand =>
      val replyTo = sender()
      val commandContext = TestStreamActorContext(command, replyTo)
      sendToStream(commandContext, stream)
  }
}
