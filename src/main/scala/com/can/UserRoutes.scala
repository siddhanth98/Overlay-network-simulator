package com.can

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import com.can.Node.{DataActionResponse, DataResponseFailed, DataResponseSuccess, DataStorageResponseSuccess}
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import scala.concurrent.Future
import JsonFormats._
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route

import scala.concurrent.duration.DurationInt

class UserRoutes()(implicit val system: ActorSystem[_]) {
  val config: Config = ConfigFactory.parseFile(new File("src/main/resources/application.conf"))
  val timeoutValue: Int = config.getInt("my-app.routes.ask-timeout")
  private implicit val timeout: Timeout = timeoutValue.seconds

  def storeMovie(movie: Node.Movie, entityId: String): Future[DataActionResponse] = {
    val parent = ClusterSharding(system).entityRefFor(Parent.typeKey, entityId)
    parent.ask(Parent.FindNodeForStoringData(_, movie.name, movie.size, movie.genre))
  }

  def getMovie(name: String, entityId: String): Future[DataActionResponse] = {
    val parent = ClusterSharding(system).entityRefFor(Parent.typeKey, entityId)
    parent.ask(Parent.FindSuccessorForFindingData(_, name))
  }

  val routes: Route =
    pathPrefix("movies" / LongNumber) { entityId =>
      concat(
        pathEnd {
          post {
            entity(as[Node.Movie]) {movie =>
              onSuccess(storeMovie(movie, entityId.toString)){response =>
                complete((StatusCodes.Created, response.asInstanceOf[DataStorageResponseSuccess]))
              }
            }
          }
        },

        pathPrefix("getMovie") {
          path(Segment) {name =>
            get {
              rejectEmptyResponse {
                onSuccess(getMovie(name, entityId.toString)) { response =>
                  //noinspection TypeCheckCanBeMatch
                  if (response.isInstanceOf[DataResponseSuccess]) complete(response.asInstanceOf[DataResponseSuccess])
                  else complete(response.asInstanceOf[DataResponseFailed])
                }
              }
            }
          }
        },

        pathEnd {
          get {
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
              "<center><h1>Hello CAN user! Please upload / download some movies.</h1></center>"))
          }
        }
      )
    }
}
