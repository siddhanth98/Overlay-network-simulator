package com.chord

import java.io.{File, FileWriter}

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object Counter {
  trait Command
  final case object Success extends Command
  final case object Failure extends Command
  final case object Finish extends Command

  def apply(): Behavior[Command] = process(0, 0)

  def process(successCount: Int, failCount: Int): Behavior[Command] =
    Behaviors.receive{
      case (context, Success) =>
        context.log.info(s"${context.self.path}\t:\tReceived success from client")
        process(successCount+1, failCount)

      case (context, Failure) =>
        context.log.info(s"${context.self.path}\t:\tReceived failure from client")
        process(successCount, failCount+1)

      case (context, Finish) =>
        context.log.info(s"${context.self.path}\t:\tGot finish message from my client. Dumping my state...")
        dumpState(successCount, failCount)
        Behaviors.stopped
    }

  def dumpState(successCount: Int, failCount: Int): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/client_data.yml"), true)
    outputFile.write(s"\nsuccess(movies successfully found) count = $successCount ; " +
      s"failure(movies could not be found) count = $failCount\n")
    outputFile.close()
  }
}
