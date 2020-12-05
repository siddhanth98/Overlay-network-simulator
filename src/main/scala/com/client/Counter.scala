package com.client

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.simulation.Aggregator

/**
 * This actor keeps track of the number of successful and failed requests made by its parent client actor
 * and sends the results to another actor which aggregates results of all clients
 */
object Counter {
  trait Command
  final case object Success extends Command
  final case object Failure extends Command
  final case object Finish extends Command

  def apply(clientRef: ActorRef[HttpClient.Command], aggregator: ActorRef[Aggregator.Aggregate]): Behavior[Command] =
    process(clientRef, aggregator, 0, 0)

  /**
   * Defines the behavior of the counter actor
   * @param clientRef Reference of the parent client actor
   * @param aggregator Reference of the aggregator actor to which the counter will send its results
   *                   once simulation finishes
   * @param successCount number of successful requests (query requests which successfully found movies) made till now
   * @param failCount number of failed requests (query requests which could not find the movie in the chord ring) made
   *                  till now
   */
  def process(clientRef: ActorRef[HttpClient.Command], aggregator: ActorRef[Aggregator.Aggregate],
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
}
