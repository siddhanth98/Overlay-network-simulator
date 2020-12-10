package com.simulation

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.client.HttpClient

import java.io.{File, FileWriter}

/**
 * This actor collects total number of successful and failed requests from all client counters once simulation ends and
 * writes the results to disk
 */
object Aggregator {
  trait Command
  final case class Aggregate(clientRef: ActorRef[HttpClient.Command], totalSuccesses: Int, totalFailures: Int) extends Command

  def apply(numberOfClients: Int): Behavior[Command] = process(Map.empty, numberOfClients, 0, 0, 0)

  /**
   * Defines the behavior of the aggregator actor
   * @param clientData Map of (client actor ref -> List(total success count, total fail count))
   * @param numberOfClients total number of clients in the simulation
   * @param clientsAggregated total number of clients whose results have been sent by their counters till now
   * @param totalSuccesses total number of successful requests across all clients
   * @param totalFailures total number of failed requests across all clients
   */
  def process(clientData: Map[ActorRef[HttpClient.Command], List[Int]], numberOfClients: Int, clientsAggregated: Int,
              totalSuccesses: Int, totalFailures: Int): Behavior[Command] =
    Behaviors.receive{
      /*
      * The aggregator has received the total success and fail counts from one of the client counters
      * If all client counters have finished sending, then the aggregator will write the results to disk,
      * otherwise it will wait for the rest of the results to arrive
      */
      case (context, Aggregate(client, ts, tf)) =>
        context.log.info(s"${context.self.path}\t:\tGot aggregate ($ts, $tf)from counter of client $client")
        if (clientsAggregated == numberOfClients-1) {
          context.log.info(s"${context.self.path}\t:\tGot aggregates from all counters. Writing result to disk.")
          writeClientDataToDisk(clientData+(client -> List(ts, tf)))
          writeAggregatedOutputToDisk(totalSuccesses+ts, totalFailures+tf)
          Behaviors.stopped
        } else process(clientData+(client -> List(ts, tf)), numberOfClients, clientsAggregated+1, totalSuccesses+ts, totalFailures+tf)
    }

  /**
   * This function will write the aggregated success/fail counts of all clients to disk
   * @param clientData Map of (client actor ref -> List(total success count, total fail count))
   */
  def writeClientDataToDisk(clientData: Map[ActorRef[HttpClient.Command], List[Int]]): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/client_data.txt"), true)
    outputFile.append("---------------------------------------------------------------------------------------------\n\n")
    clientData.keySet.foreach(client =>
      outputFile.append(s"\nClient - $client\nSuccess Count = ${clientData(client).head}\tFailure Count = ${clientData(client)(1)}\n\n")
    )
    outputFile.append("---------------------------------------------------------------------------------------------\n\n")
    outputFile.close()
  }

  /**
   * This function will write the total number of success/fail % across all clients to disk
   * @param totalSuccesses total number of successful requests made by all client actors
   * @param totalFailures total number of failed requests made by all client actors
   */
  def writeAggregatedOutputToDisk(totalSuccesses: Int, totalFailures: Int): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/aggregate.txt"))
    outputFile.append(s"Success = ${100 * (totalSuccesses.asInstanceOf[Double]/(totalSuccesses+totalFailures).asInstanceOf[Double])}%\n")
    outputFile.append(s"Failure = ${100 * (totalFailures.asInstanceOf[Double]/(totalSuccesses+totalFailures).asInstanceOf[Double])}%\n")
    outputFile.close()
  }
}
