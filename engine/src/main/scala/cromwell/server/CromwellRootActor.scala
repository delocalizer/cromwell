package cromwell.server

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, ActorRef, OneForOneStrategy}
import akka.event.Logging
import akka.routing.RoundRobinPool
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import cromwell.core.Dispatcher
import cromwell.core.actor.StreamIntegration.EnqueuingException
import cromwell.core.io.Throttle
import cromwell.engine.backend.{BackendSingletonCollection, CromwellBackends}
import cromwell.engine.io.{IoActor, IoFlow}
import cromwell.engine.workflow.WorkflowManagerActor
import cromwell.engine.workflow.lifecycle.CopyWorkflowLogsActor
import cromwell.engine.workflow.lifecycle.execution.callcaching.{CallCache, CallCacheReadActor}
import cromwell.engine.workflow.tokens.JobExecutionTokenDispenserActor
import cromwell.engine.workflow.workflowstore.{SqlWorkflowStore, WorkflowStore, WorkflowStoreActor}
import cromwell.jobstore.{JobStore, JobStoreActor, SqlJobStore}
import cromwell.services.{ServiceRegistryActor, SingletonServicesStore}
import cromwell.subworkflowstore.{SqlSubWorkflowStore, SubWorkflowStoreActor}
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * An actor which serves as the lord protector for the rest of Cromwell, allowing us to have more fine grain
  * control on top level supervision, etc.
  *
  * For now there are only two (non-test) entry points into Cromwell - either using SingleWorkflowRunnerActor or CromwellServerActor.
  * This is intended to be extended by those entry points.
  *
  * If any of the actors created by CromwellRootActor fail to initialize the ActorSystem will die, which means that
  * Cromwell will fail to start in a bad state regardless of the entry point.
  */
 abstract class CromwellRootActor extends Actor {
  import CromwellRootActor._

  private val logger = Logging(context.system, this)
  private val config = ConfigFactory.load()
  val serverMode: Boolean

  implicit val materializer = ActorMaterializer()
  lazy val ioEc = context.system.dispatchers.lookup(Dispatcher.IoDispatcher)
  
  lazy val systemConfig = config.getConfig("system")
  lazy val serviceRegistryActor: ActorRef = context.actorOf(ServiceRegistryActor.props(config), "ServiceRegistryActor")
  lazy val numberOfWorkflowLogCopyWorkers = systemConfig.as[Option[Int]]("number-of-workflow-log-copy-workers").getOrElse(DefaultNumberOfWorkflowLogCopyWorkers)

  lazy val workflowLogCopyRouter: ActorRef = context.actorOf(RoundRobinPool(numberOfWorkflowLogCopyWorkers)
      .withSupervisorStrategy(CopyWorkflowLogsActor.strategy)
      .props(CopyWorkflowLogsActor.props(serviceRegistryActor)),
      "WorkflowLogCopyRouter")

  lazy val workflowStore: WorkflowStore = SqlWorkflowStore(SingletonServicesStore.databaseInterface)
  lazy val workflowStoreActor = context.actorOf(WorkflowStoreActor.props(workflowStore, serviceRegistryActor), "WorkflowStoreActor")

  lazy val jobStore: JobStore = new SqlJobStore(SingletonServicesStore.databaseInterface)
  lazy val jobStoreActor = context.actorOf(JobStoreActor.props(jobStore), "JobStoreActor")

  lazy val subWorkflowStore = new SqlSubWorkflowStore(SingletonServicesStore.databaseInterface)
  lazy val subWorkflowStoreActor = context.actorOf(SubWorkflowStoreActor.props(subWorkflowStore), "SubWorkflowStoreActor")
  
  // Flow version
  lazy val ioParallelism = systemConfig.as[Option[Int]]("io.parallelism").getOrElse(100)
  lazy val throttleElements = systemConfig.as[Option[Int]]("io.number-of-requests").getOrElse(100000)
  lazy val throttlePer = systemConfig.as[Option[FiniteDuration]]("io.per").getOrElse(100 seconds)
  lazy val ioThrottle = Throttle(throttleElements, throttlePer, throttleElements)
  lazy val ioFlow = new IoFlow(ioParallelism, context.system.scheduler, Option(ioThrottle))(ioEc)
  lazy val ioActor = context.actorOf(IoActor.props(1000, ioFlow).withDispatcher(Dispatcher.IoDispatcher))

  lazy val callCache: CallCache = new CallCache(SingletonServicesStore.databaseInterface)
  lazy val callCacheReadActor = context.actorOf(RoundRobinPool(25)
    .props(CallCacheReadActor.props(callCache)),
    "CallCacheReadActor")

  lazy val backendSingletons = CromwellBackends.instance.get.backendLifecycleActorFactories map {
    case (name, factory) => name -> (factory.backendSingletonActorProps map context.actorOf)
  }
  lazy val backendSingletonCollection = BackendSingletonCollection(backendSingletons)

  lazy val jobExecutionTokenDispenserActor = context.actorOf(JobExecutionTokenDispenserActor.props)

  def abortJobsOnTerminate: Boolean

  lazy val workflowManagerActor = context.actorOf(
    WorkflowManagerActor.props(
      workflowStoreActor, ioActor, serviceRegistryActor, workflowLogCopyRouter, jobStoreActor, subWorkflowStoreActor, callCacheReadActor,
      jobExecutionTokenDispenserActor, backendSingletonCollection, abortJobsOnTerminate, serverMode),
    "WorkflowManagerActor")

  override def receive = {
    case _ => logger.error("CromwellRootActor is receiving a message. It prefers to be left alone!")
  }

  /**
    * Validate that all of the direct children actors were successfully created, otherwise error out the initialization
    * of Cromwell by passing a Throwable to the guardian.
    */
  override val supervisorStrategy = OneForOneStrategy() {
    case actorInitializationException: ActorInitializationException => Escalate
    case enqueuing: EnqueuingException => Restart
    case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
  }
}

object CromwellRootActor {
  val DefaultNumberOfWorkflowLogCopyWorkers = 10
}
