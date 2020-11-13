package com.chord

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import JsonFormats._
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.chord.Server.{AllData, Command, Data, DataActionResponse, DataResponseSuccess, DataStorageResponseSuccess, FindNodeForStoringData, FindSuccessorToFindData, GetAllData}

import scala.concurrent.Future

class UserRoutes (server: ActorRef[Command])(implicit val system: ActorSystem[_]) {
  private implicit val timeout: Timeout = Timeout.create(system.settings.config.getDuration("my-app.routes.ask-timeout"))

  def createMovie(data: Data): Future[DataActionResponse] =
    server.ask(FindNodeForStoringData(data, _))

  def getAllMovies: Future[AllData] =
    server.ask(GetAllData)

  def getMovie(name: String): Future[DataActionResponse] =
    server.ask(FindSuccessorToFindData(name, _))

  val createMovieRoute: Route =
    pathEnd {
      post {
        entity(as[Data]) { movie =>
          onSuccess(createMovie(movie)) { response =>
            complete((StatusCodes.Created, response.asInstanceOf[DataStorageResponseSuccess]))
          }
        }
      }
    }

  val getAllMoviesRoute: Route =
    pathEnd  {
      get {
        complete(getAllMovies)
      }
    }

  val getMovieRoute: Route =
    path(Segment) { movieName =>
      get {
        onSuccess(getMovie(movieName)) { response => complete(response.asInstanceOf[DataResponseSuccess])
        }
      }
    }

  val userRoutes: Route =
    pathPrefix("movies") {
      concat(createMovieRoute, getAllMoviesRoute, getMovieRoute)
    }
}
