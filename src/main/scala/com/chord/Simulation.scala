package com.chord

import java.util.{Timer, TimerTask}

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.Random

/**
 * The simulation actor will spawn all client actors and run a simulation for the specified amount of time.
 * On every time tick, the simulation will randomly choose a client to make a request, and then again generate a
 * random number, which, if even, will make the client make a post request otherwise a get request.
 */
object Simulation {
  trait Command
  final case object CreateClients extends Command
  final case class SimulationMovie(name: String, size: Int, genre: String) extends Command
  final val logger: Logger = LoggerFactory.getLogger(Simulation.getClass)

  def apply(numberOfClients: Int, simulationTime: Int, maxRequestsPerMin: Int): Behavior[Command] = {

    /**
     * Defines the behavior of the simulation actor
     * @param movies Set of movies which have either been stored/queried by the clients.
     *               Movie to query is a randomly chosen movie from this set
     * @param numberOfClients Number of clients to spawn in the simulation
     */
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

              /**
               * This function defines the task which will execute as part of the simulation for the specified amount
               * of time in periodic ticks.
               * This tick depends on the maximum number of requests which can be made per client per minute from the
               * input configuration.
               * Each task involves generating a movie with a random name, size and genre, then randomly selecting a client
               * to make a request, then again randomly selecting between a POST/GET request to be made.
               * If it is a post request, then the randomly generated is stored.
               * If it is a get request, then the movie to query is randomly selected from the set of movies stored by the
               * simulation actor.
               */
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