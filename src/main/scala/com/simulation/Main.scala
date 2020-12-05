package com.simulation

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory

import java.io.File

/**
 * This is the main driver object of the client side simulation (separate from the http server main driver)
 * It creates the actor system whose guardian behavior is that of the simulation actor.
 */
object Main {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/clientconfig.conf"))
    val numberOfClients = config.getInt("conf.NUMBER_OF_CLIENTS")
    val simulationTime = config.getInt("conf.SIMULATION_TIME_IN_SECONDS")
    val maxRequestsPerMinute = config.getInt("conf.MAX_REQUESTS_PER_MIN")

    val guardianActor = ActorSystem(Simulation(numberOfClients, simulationTime, maxRequestsPerMinute), "SimulationActorSystem")
    guardianActor ! Simulation.CreateClients
  }
}
