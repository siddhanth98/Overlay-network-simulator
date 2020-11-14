package com.chord

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}


object Main {
  def main(args: Array[String]): Unit = {
    val guardianActor = ActorSystem(Simulation(50), "SimulationActorSystem")
    guardianActor ! Simulation.CreateClients
  }
}
