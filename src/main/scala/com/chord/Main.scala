package com.chord

import java.io.File

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory


object Main {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseFile(new File("src/main/resources/clientconfig.conf"))
    val numberOfClients = config.getInt("conf.NUMBER_OF_CLIENTS")
    val simulationTime = config.getInt("conf.SIMULATION_TIME_IN_SECONDS")
    val maxRequestsPerMinute = config.getInt("conf.MAX_REQUESTS_PER_MIN")

    val guardianActor = ActorSystem(Simulation(numberOfClients, simulationTime, maxRequestsPerMinute), "SimulationActorSystem")
    guardianActor ! Simulation.CreateClients
  }
}
