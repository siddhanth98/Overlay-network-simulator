package com.can

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import com.can.Node.{DataActionResponse, DataResponseSuccess, DataStorageResponseSuccess}
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import scala.concurrent.Future
import JsonFormats._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route

class UserRoutes(parent: ActorRef[Parent.Command])(implicit val system: ActorSystem[_]) {
  val config: Config = ConfigFactory.parseFile(new File("src/main/resources/configuration/application.conf"))
  private implicit val timeout: Timeout = Timeout.create(config.getDuration("my-app.routes.ask-timeout"))

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
              complete(response.asInstanceOf[DataResponseSuccess])
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
