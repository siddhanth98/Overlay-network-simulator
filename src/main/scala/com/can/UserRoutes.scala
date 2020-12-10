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
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route

import scala.concurrent.duration.DurationInt

class UserRoutes(parent: ActorRef[Parent.Command])(implicit val system: ActorSystem[_]) {
  val config: Config = ConfigFactory.parseFile(new File("src/main/resources/configuration/application.conf"))
  private implicit val timeout: Timeout = 5.seconds

  def storeMovie(movie: Node.Movie): Future[DataActionResponse] =
    parent.ask(Parent.FindNodeForStoringData(_, movie.name, movie.size, movie.genre))

  def getMovie(name: String): Future[DataActionResponse] =
    parent.ask(Parent.FindSuccessorForFindingData(_, name))

  val storeMovieRoute: Route =
    pathEnd {
      post {
        entity(as[Node.Movie]) {movie =>
          onSuccess(storeMovie(movie)){response =>
            complete((StatusCodes.Created, response.asInstanceOf[DataStorageResponseSuccess]))
          }
        }
      }
    }

  val getMovieRoute: Route =
    pathPrefix("getMovie") {
      path(Segment) {name =>
        get {
          rejectEmptyResponse {
            onSuccess(getMovie(name)) { response =>
              //noinspection TypeCheckCanBeMatch
              if (response.isInstanceOf[DataResponseSuccess]) complete(response.asInstanceOf[DataResponseSuccess])
              else complete(response.asInstanceOf[DataResponseFailed])
            }
          }
        }
      }
    }

  val homePageRoute: Route =
    pathEnd {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
          "<center><h1>Hello CAN user! Please upload / download some movies.</h1></center>"))
      }
    }

  val routes: Route =
    pathPrefix("movies") {
      concat(
        storeMovieRoute, getMovieRoute, homePageRoute
      )
    }
}
