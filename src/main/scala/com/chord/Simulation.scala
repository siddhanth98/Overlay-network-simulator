package com.chord

import java.time.LocalTime
import java.util.{Timer, TimerTask}

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.Random

object Simulation {
  trait Command
  final case object CreateClients extends Command
  final case class SimulationMovie(name: String, size: Int, genre: String) extends Command
  final val logger: Logger = LoggerFactory.getLogger(Simulation.getClass)

  def apply(numberOfClients: Int, simulationTime: Int, maxRequestsPerMin: Int): Behavior[Command] = {
    def update(movies: mutable.Set[SimulationMovie], numberOfClients: Int): Behavior[Command] = {
      Behaviors.setup { context =>
        context.log.info(s"${context.self.path}\t:\tSimulation starting...\nCreating client actors...")
        val genreList = List("Action", "Action-Thriller", "Comedy", "romantic-Comedy")

        Behaviors.receiveMessage {
          case CreateClients =>
            val clientActors = mutable.ListBuffer[ActorRef[HttpClient.Command]]()
            (1 to numberOfClients).foreach(i => {
              val clientActor = context.spawn(HttpClient(), s"client$i")
              clientActors.append(clientActor)
            })

            val startTime = LocalTime.now().getSecond
            val timer = new Timer()
            val timerTask = new TimerTask {
              override def run(): Unit = {
                val movieName = s"Movie${scala.util.Random.nextInt(1000)}"
                val movieSize = scala.util.Random.nextInt(1000)
                val movieGenre = genreList(scala.util.Random.nextInt(genreList.size))
                movies += SimulationMovie(movieName, movieSize, movieGenre)

                val randomClient = clientActors(Random.nextInt(clientActors.size))
                logger.info(s"${context.self.path}\t:\tSelected client $randomClient to make a request")
                if (Random.nextInt(10000) % 2 == 0)
                  randomClient ! HttpClient.PostMovie(movieName, movieSize, movieGenre)
                else randomClient ! HttpClient.GetMovie(movieName)

                if ((LocalTime.now().getSecond - startTime) >= simulationTime) timer.cancel()
              }
            }
            timer.schedule(timerTask, 0, (60/maxRequestsPerMin)*1000)
            Behaviors.same
        }
      }
    }
    HttpServer()
    update(mutable.Set.empty, numberOfClients)
  }
}