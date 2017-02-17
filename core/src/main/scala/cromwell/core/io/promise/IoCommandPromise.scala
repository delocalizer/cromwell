package cromwell.core.io.promise

import better.files.File._
import cromwell.core.io.IoActorCommand._
import cromwell.core.io._
import cromwell.core.path.Path
import cromwell.core.retry.SimpleExponentialBackoff

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration


sealed trait IoActorPromiseCommand[T] { this: IoCommand[T] =>
  def promise: Promise[T]
  override def fail(failure: Throwable) = {
    promise.tryFailure(failure)
    None
  }
  override def success(value: T) = {
    promise.trySuccess(value)
    None
  }

  override def retry(waitTime: FiniteDuration) = None
}

/**
  * Copy source -> destination
  */
case class CopyCommandPromise(file: Path,
                              destination: Path,
                              overwrite: Boolean = true,
                              backoff: SimpleExponentialBackoff = defaultBackoff)(val promise: Promise[Unit]) extends IoActorPromiseCommand[Unit] with CopyCommand {
  override def withNextBackoff: CopyCommandPromise = copy(backoff = backoff.next)(promise)
}

/**
  * Read file as a string (load the entire content in memory)
  */
case class ReadAsStringCommandPromise(file: Path,
                                      backoff: SimpleExponentialBackoff = defaultBackoff)(val promise: Promise[String]) extends IoActorPromiseCommand[String] with ReadAsStringCommand {
  override def withNextBackoff: ReadAsStringCommandPromise = copy(backoff = backoff.next)(promise)
}

/**
  * Return the size of file
  */
case class SizeCommandPromise(file: Path,
                              backoff: SimpleExponentialBackoff = defaultBackoff)(val promise: Promise[Long]) extends IoActorPromiseCommand[Long] with SizeCommand {
  override def withNextBackoff: SizeCommandPromise = copy(backoff = backoff.next)(promise)
}

/**
  * Write content in file
  */
case class WriteCommandPromise(file: Path,
                               content: String,
                               openOptions: OpenOptions,
                               backoff: SimpleExponentialBackoff = defaultBackoff)(val promise: Promise[Unit]) extends IoActorPromiseCommand[Unit] with WriteCommand {
  override def withNextBackoff: WriteCommandPromise = copy(backoff = backoff.next)(promise)
}

/**
  * Delete file
  */
case class DeleteCommandPromise(file: Path,
                                swallowIOExceptions: Boolean,
                                backoff: SimpleExponentialBackoff = defaultBackoff)(val promise: Promise[Unit]) extends IoActorPromiseCommand[Unit] with DeleteCommand {
  override def withNextBackoff: DeleteCommandPromise = copy(backoff = backoff.next)(promise)
}