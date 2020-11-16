package com.chord

import java.io.{File, FileWriter}
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
        context.log.info(s"${context.self.path}\t:\tSimulation will run for $simulationTime seconds")
        val genreList = List("Action", "Action-Thriller", "Comedy", "romantic-Comedy")
        val aggregator = context.spawn(Aggregator(numberOfClients), "ClientCounterAggregator")

        Behaviors.receiveMessage {
          case CreateClients =>
            val clientActors = mutable.ListBuffer[ActorRef[HttpClient.Command]]()
            (1 to numberOfClients).foreach(i => {
              val clientActor = context.spawn(HttpClient(aggregator), s"client$i")
              clientActors.append(clientActor)
            })

            val startTime = System.currentTimeMillis() / 1000
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
                else randomClient ! HttpClient.GetMovie(movies.toList(Random.nextInt(movies.size)).name)

                if (Math.abs(System.currentTimeMillis() / 1000 -startTime) >= simulationTime) {
                  logger.info(s"${context.self.path}\t:\tFinishing simulation...")
                  val outputFile = new FileWriter(new File("src/main/resources/outputs/client_data.txt"))
                  outputFile.write(s"\n----------------------- Simulation results ----------------------")
                  outputFile.write(s"\nsimulationTime = $simulationTime\nmaxRequestsPerMinute = $maxRequestsPerMin")
                  clientActors.foreach(client => client ! HttpClient.FinishSimulation)
                  timer.cancel()
                }
              }
            }
            timer.schedule(timerTask, 0, Math.ceil(60D/maxRequestsPerMin.asInstanceOf[Double]).toInt*1000)
            Behaviors.same
        }
      }
    }
    update(mutable.Set.empty, numberOfClients)
  }
}