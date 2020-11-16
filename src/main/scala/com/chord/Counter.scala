package com.chord

import java.io.{File, FileWriter}

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object Counter {
  trait Command
  final case object Success extends Command
  final case object Failure extends Command
  final case object Finish extends Command

  def apply(clientRef: ActorRef[HttpClient.Command], aggregator: ActorRef[Aggregator.Command]): Behavior[Command] =
    process(clientRef, aggregator, 0, 0)

  def process(clientRef: ActorRef[HttpClient.Command], aggregator: ActorRef[Aggregator.Command],
              successCount: Int, failCount: Int): Behavior[Command] =
    Behaviors.receive{
      case (context, Success) =>
        context.log.info(s"${context.self.path}\t:\tReceived success from client $clientRef")
        process(clientRef, aggregator, successCount+1, failCount)

      case (context, Failure) =>
        context.log.info(s"${context.self.path}\t:\tReceived failure from client $clientRef")
        process(clientRef, aggregator, successCount, failCount+1)

      case (context, Finish) =>
        context.log.info(s"${context.self.path}\t:\tGot finish message from client $clientRef. Sending data to aggregator...")
        aggregator ! Aggregator.Aggregate(clientRef, successCount, failCount)
        clientRef ! HttpClient.FinishCounter
        Behaviors.stopped
    }

  def dumpState(clientRef: ActorRef[HttpClient.Command], successCount: Int, failCount: Int): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/client_data.txt"), true)
    outputFile.append(s"\n$clientRef:\nsuccess(movies successfully found) count = $successCount ; " +
      s"failure(movies could not be found) count = $failCount\n")
    outputFile.close()
  }
}
