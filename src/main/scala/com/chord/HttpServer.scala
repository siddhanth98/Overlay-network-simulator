package com.chord

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route

import scala.util.{Failure, Success}

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
   * The guardian will spawn required number of child server actors
   * that form part of the chord ring.
   * Then link the user routes to each of the actors and
   * start the http server using the helper method above
   */
  def main(args: Array[String]): Unit = {
    val guardianBehavior = Behaviors.setup[Nothing] {context =>
      val parentActor = context.spawn(Parent(4, 10), "Parent")
      context.log.info(s"${context.self.path}\t:\tSpawned parent actor - $parentActor")
      val userRoutes = new UserRoutes(parentActor)(context.system)
      startHttpServer(userRoutes.userRoutes)(context.system)
      Behaviors.empty
    }
    val system = ActorSystem[Nothing](guardianBehavior, "ChordActorSystem")
  }
}
