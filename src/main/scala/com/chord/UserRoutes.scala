package com.chord

import java.io.File

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import JsonFormats._
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.chord.Server.{AllData, Data, DataActionResponse, DataResponseFailed, DataResponseSuccess, DataStorageResponseSuccess, FindNodeForStoringData, FindSuccessorToFindData, GetAllData}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Future

/**
 * This class creates the route objects which represent the REST endpoints for the chord server.
 * It will receive GET/POST requests on a particular route and send those requests to the parent actor of
 * the chord nodes in the ring.
 * @param parent Parent actor ref
 * @param system Actor system of the chord server
 */
class UserRoutes (parent: ActorRef[Parent.Command])(implicit val system: ActorSystem[_]) {
  val config: Config = ConfigFactory.parseFile(new File("src/main/resources/configuration/application.conf"))
  private implicit val timeout: Timeout = Timeout.create(config.getDuration("my-app.routes.ask-timeout"))

  /**
   * This function "asks" the parent actor to find a node for storing the requested movie and returns a future of
   * the corresponding response format
   * @param data The movie to be stored
   */
  def createMovie(data: Data): Future[DataActionResponse] =
    parent.ask(FindNodeForStoringData(data, _))

  /**
   * This function "asks" the parent actor to get all movies from a node in the ring
   */
  def getAllMovies: Future[AllData] =
    parent.ask(GetAllData)

  /**
   * This function "asks" the parent node to find the node in the ring storing the requested movie
   * @param name Name of the movie
   */
  def getMovie(name: String): Future[DataActionResponse] =
    parent.ask(FindSuccessorToFindData(name, _))

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

  val getMovieRoute: Route = {
    pathPrefix("getMovie") {
      path(Segment) { movieName =>
        get {
          onSuccess(getMovie(movieName)) { response => {
            //noinspection TypeCheckCanBeMatch
            if (response.isInstanceOf[DataResponseSuccess])
              complete(response.asInstanceOf[DataResponseSuccess])
            else
              complete(response.asInstanceOf[DataResponseFailed])
          }
          }
        }
      }
    }
  }

  val homePageRoute: Route =
    pathEnd {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<center><h1>Hello! Please upload/download some movies</h1></center>"))
      }
    }

  val userRoutes: Route =
    pathPrefix("movies") {
      concat(createMovieRoute, getMovieRoute, homePageRoute)
    }
}
