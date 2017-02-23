package cromwell.core.io

import better.files.File.OpenOptions
import com.google.api.client.util.{BackOff, ExponentialBackOff}
import cromwell.core.io.IoCommand._
import cromwell.core.path.Path
import cromwell.core.retry.SimpleExponentialBackoff

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps
import scala.util.Try

object IoCommand {
  def defaultGoogleBackoff = new ExponentialBackOff.Builder()
    .setInitialIntervalMillis((1 second).toMillis.toInt)
    .setMaxIntervalMillis((5 minutes).toMillis.toInt)
    .setMultiplier(3L)
    .setRandomizationFactor(0.2D)
    .setMaxElapsedTimeMillis((10 minutes).toMillis.toInt)
    .build()
  def defaultBackoff = SimpleExponentialBackoff(defaultGoogleBackoff)
  
  type RetryCommand[T] = (FiniteDuration, IoCommand[T])
}

sealed trait IoCommand[T] {
  /**
    * Completes the command successfully
    * @return a message to be sent back to the sender, if needed
    */
  def success(value: T): IoSuccess[T] = IoSuccess(this, value)
  
  /**
    * Fail the command with an exception
    */
  def fail(failure: Throwable): IoFailure[T] = IoFailure(this, failure)

  def withNextBackoff: IoCommand[T]

  def backoff: SimpleExponentialBackoff

  /**
    * Current value of the backoff duration or None if the command should not be retried
    */
 def timeBeforeNextTry: Option[FiniteDuration] = backoff.backoffMillis match {
    case BackOff.STOP => None
    case value => Option(value millis)
  }
}

sealed trait IoBatchCommand[T] extends IoCommand[List[T]] {
  def execute(): Unit
  def get: List[Try[T]]
}

trait IoBatchHashCommand extends IoBatchCommand[String]

/**
  * Io Command
  *
  * @tparam T type of the value to be returned after the command is executed
  */
sealed trait IoSingleCommand[T] extends IoCommand[T]

/**
  * Copy source -> destination
  */
case class IoCopyCommand(file: Path,
                              destination: Path,
                              overwrite: Boolean,
                              backoff: SimpleExponentialBackoff = defaultBackoff
                             ) extends IoSingleCommand[Unit] {
  override def withNextBackoff = copy(backoff = backoff.next)
}

/**
  * Read file as a string (load the entire content in memory)
  */
case class IoContentAsStringCommand(file: Path,
                                    backoff: SimpleExponentialBackoff = defaultBackoff
                                     ) extends IoSingleCommand[String] {
  override def withNextBackoff = copy(backoff = backoff.next)
}

/**
  * Return the size of file
  */
case class IoSizeCommand(file: Path,
                              backoff: SimpleExponentialBackoff = defaultBackoff
                             ) extends IoSingleCommand[Long] {
  override def withNextBackoff = copy(backoff = backoff.next)
}

/**
  * Write content in file
  */
case class IoWriteCommand(file: Path,
                               content: String,
                               openOptions: OpenOptions,
                               backoff: SimpleExponentialBackoff = defaultBackoff
                              ) extends IoSingleCommand[Unit] {
  override def withNextBackoff = copy(backoff = backoff.next)
}

/**
  * Delete file
  */
case class IoDeleteCommand(file: Path,
                                swallowIOExceptions: Boolean,
                                backoff: SimpleExponentialBackoff = defaultBackoff
                               ) extends IoSingleCommand[Unit] {
  override def withNextBackoff = copy(backoff = backoff.next)
}
