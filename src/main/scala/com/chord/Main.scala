package com.chord

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

object Simulation {
  trait Command
  final case object CreateClients extends Command
  final case class SimulationMovie(name: String, size: Int, genre: String) extends Command

  def apply(numberOfClients: Int): Behavior[Command] = {
    def update(movies: Set[SimulationMovie], numberOfClients: Int): Behavior[Command] = {
      Behaviors.setup { context =>
        context.log.info(s"${context.self.path}\t:\tSimulation starting...\nCreating client actors...")
        val genreList = List("Action", "Action-Thriller", "Comedy", "romantic-Comedy")

        Behaviors.receiveMessage {
          case CreateClients =>
            (1 to numberOfClients).foreach(i => {
              val movieName = s"Movie${scala.util.Random.nextInt(1000)}"
              val movieSize = scala.util.Random.nextInt(1000)
              val movieGenre = genreList(scala.util.Random.nextInt(genreList.size))
              val clientActor = context.spawn(HttpClient(), s"client$i")
              clientActor ! HttpClient.PostMovie(movieName, movieSize, movieGenre)
            })
            Behaviors.same
        }
      }
    }
    update(Set.empty, numberOfClients)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val guardianActor = ActorSystem(Simulation(50), "SimulationActorSystem")
    guardianActor ! Simulation.CreateClients
  }
}
