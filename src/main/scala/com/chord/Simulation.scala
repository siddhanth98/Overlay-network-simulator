package com.chord

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

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

              /* If random number generated is even then post the movie, otherwise query server to find the movie */
              if (scala.util.Random.nextInt(100000) % 2 == 0) {
                context.log.info(s"${context.self.path}\t:\tTelling client to make a post request")
                clientActor ! HttpClient.PostMovie(movieName, movieSize, movieGenre)
              } else {
                context.log.info(s"${context.self.path}\t:\tTelling client to make a get request")
                clientActor ! HttpClient.GetMovie(movieName)
              }
            })
            Behaviors.same
        }
      }
    }
    update(Set.empty, numberOfClients)
  }
}