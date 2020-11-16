package com.chord

import java.io.{File, FileWriter}

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object Aggregator {
  trait Command
  final case class Aggregate(clientRef: ActorRef[HttpClient.Command], totalSuccesses: Int, totalFailures: Int) extends Command

  def apply(numberOfClients: Int): Behavior[Command] = process(Map.empty, numberOfClients, 0, 0, 0)

  def process(clientData: Map[ActorRef[HttpClient.Command], List[Int]], numberOfClients: Int, clientsAggregated: Int,
              totalSuccesses: Int, totalFailures: Int): Behavior[Command] =
    Behaviors.receive{
      case (context, Aggregate(client, ts, tf)) =>
        context.log.info(s"${context.self.path}\t:\tGot aggregate ($ts, $tf)from counter of client $client")
        if (clientsAggregated == numberOfClients-1) {
          context.log.info(s"${context.self.path}\t:\tGot aggregates from all counters. Writing result to disk.")
          writeClientDataToDisk(clientData+(client -> List(ts, tf)))
          writeAggregatedOutputToDisk(totalSuccesses+ts, totalFailures+tf)
          Behaviors.stopped
        } else process(clientData+(client -> List(ts, tf)), numberOfClients, clientsAggregated+1, totalSuccesses+ts, totalFailures+tf)
    }

  def writeClientDataToDisk(clientData: Map[ActorRef[HttpClient.Command], List[Int]]): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/client_data.txt"), true)
    clientData.keySet.foreach(client =>
      outputFile.append(s"\nClient - $client\nSuccess Count = ${clientData(client).head}\tFailure Count = ${clientData(client)(1)}\n\n")
    )
    outputFile.close()
  }

  def writeAggregatedOutputToDisk(totalSuccesses: Int, totalFailures: Int): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/aggregate.txt"))
    outputFile.append(s"Success = ${100 * (totalSuccesses.asInstanceOf[Double]/(totalSuccesses+totalFailures).asInstanceOf[Double])}%\n")
    outputFile.append(s"Failure = ${100 * (totalFailures.asInstanceOf[Double]/(totalSuccesses+totalFailures).asInstanceOf[Double])}%\n")
    outputFile.close()
  }
}
