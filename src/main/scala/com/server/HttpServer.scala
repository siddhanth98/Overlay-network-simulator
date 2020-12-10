package com.server

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.chord.Parent
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import scala.util.{Failure, Success}

/**
 * This object starts the http server instance on port number 8080 using the created http routes
 */
object HttpServer {
  private def startHttpServer(route: Route)(implicit system: ActorSystem[_]): Unit = {
    import system.executionContext

    val futureBinding = Http().newServerAt("localhost", 8080).bind(route)
    futureBinding.onComplete {
      case Success(binding) =>
        val hostAddress = binding.localAddress
        system.log.info(s"Started server at http://${hostAddress.getHostString}:${hostAddress.getPort}...")
      case Failure(ex) =>
        system.log.error(s"failed to start server at requested port 8080. Terminating system.\t$ex")
        system.terminate()
    }
  }

  /**
   * Define the guardian behavior here.
   * The guardian will spawn the parent actor in the actor system.
   * Then all http routes are linked to the parent actor.
   */
  def apply(config: Config): Unit = {
    val guardianBehavior = Behaviors.setup[Nothing] {context =>
      val topology = config.getString("app.TOPOLOGY")
      val n = config.getInt("app.NUMBER_OF_NODES")
      val dumpPeriod = config.getInt("app.DUMP_PERIOD_IN_SEC")
      val replicationPeriod = config.getInt("app.REPLICATION_PERIOD")
      val nodeJoinFailPeriod = config.getInt("app.NODE_JOIN_FAILURE_PERIOD")
      val numberOfShards = config.getInt("app.NUMBER_OF_SHARDS")

      if (topology.equals("CHORD")) {
        val m = config.getInt("app.CHORD.NUMBER_OF_FINGERS")

        val parent = Parent.initializeShardRegion(context.system, numberOfShards, m, n, dumpPeriod, nodeJoinFailPeriod, replicationPeriod)
        context.log.info(s"${context.self.path}\t:\tSpawned parent actor - $parent")

        val userRoutes = new UserRoutes()(context.system)
        startHttpServer(userRoutes.userRoutes)(context.system)
      }
      else {
        val endX = config.getInt("app.CAN.END_X")
        val endY = config.getInt("app.CAN.END_Y")
        val parentActor = com.can.Parent.initializeShardRegion(context.system, numberOfShards, n, endX, endY, replicationPeriod, nodeJoinFailPeriod, dumpPeriod)
        context.log.info(s"${context.self.path}\t:\tSpawned parent actor - $parentActor")

        val userRoutes = new com.can.UserRoutes()(context.system)
        startHttpServer(userRoutes.routes)(context.system)
      }
      Behaviors.empty
    }
    val _ = ActorSystem[Nothing](guardianBehavior, "ServerActorSystem",
      ConfigFactory.parseString(s"""akka.remote.artery.canonical.port = 2553""")
        .withFallback(ConfigFactory.load()))
  }


  /**
   * This is the main driver function of the chord server.
   * It creates the http server object which is responsible for bootstrapping the server side actor system.
   */
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/serverconfig.conf"))
    HttpServer(config)
  }
}
