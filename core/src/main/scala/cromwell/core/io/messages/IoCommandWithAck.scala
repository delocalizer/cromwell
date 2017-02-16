package cromwell.core.io.messages

import better.files.File.OpenOptions
import cromwell.core.io._
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.core.io.IoActorCommand._
import cromwell.core.path.Path

import scala.concurrent.duration.FiniteDuration

sealed trait IoCommandWithAck[T] { this: IoCommand[T] =>
  override def success(value: T) = Option(IoSuccess(this, value))
  override def fail(failure: Throwable) = Option(IoFailure(this, failure))
  override def retry(waitTime: FiniteDuration) = Option(IoRetry(this, waitTime))
}

/**
  * Copy source -> destination
  */
case class CopyCommandMessage(file: Path,
                              destination: Path,
                              overwrite: Boolean = true,
                              backoff: SimpleExponentialBackoff = defaultBackoff
                             ) extends CopyCommand with IoCommandWithAck[Unit] {
  override def withNextBackoff: CopyCommand = copy(backoff = backoff.next)
}

/**
  * Read file as a string (load the entire content in memory)
  */
case class ReadAsStringCommandMessage(file: Path,
                                      backoff: SimpleExponentialBackoff = defaultBackoff
                                     ) extends ReadAsStringCommand with IoCommandWithAck[String] {
  override def withNextBackoff: ReadAsStringCommand = copy(backoff = backoff.next)
}

/**
  * Return the size of file
  */
case class SizeCommandMessage(file: Path,
                              backoff: SimpleExponentialBackoff = defaultBackoff
                             ) extends SizeCommand with IoCommandWithAck[Long] {
  override def withNextBackoff = copy(backoff = backoff.next)
}

/**
  * Write content in file
  */
case class WriteCommandMessage(file: Path,
                               content: String,
                               openOptions: OpenOptions,
                               backoff: SimpleExponentialBackoff = defaultBackoff
                              ) extends WriteCommand with IoCommandWithAck[Unit] {
  override def withNextBackoff = copy(backoff = backoff.next)
}

/**
  * Delete file
  */
case class DeleteCommandMessage(file: Path,
                                swallowIOExceptions: Boolean = false,
                                backoff: SimpleExponentialBackoff = defaultBackoff
                               ) extends DeleteCommand with IoCommandWithAck[Unit] {
  override def withNextBackoff = copy(backoff = backoff.next)
}