package com.server

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.chord.Parent
import com.chord.Node._
import com.server.JsonFormats._
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * This class creates the route objects which represent the REST endpoints for the chord server.
 * It will receive GET/POST requests on a particular route and send those requests to the parent actor of
 * the chord nodes in the ring.
 * @param parent Parent actor ref
 * @param system Actor system of the chord server
 */
class UserRoutes ()(implicit val system: ActorSystem[_]) {
  val config: Config = ConfigFactory.parseFile(new File("src/main/resources/application.conf"))
  private implicit val timeout: Timeout = 10.seconds

  /**
   * This function "asks" the parent actor to find a node for storing the requested movie and returns a future of
   * the corresponding response format
   * @param data The movie to be stored
   */
  def createMovie(data: Data, entityId: String): Future[DataActionResponse] = {
    val parent = ClusterSharding(system).entityRefFor(Parent.typeKey, entityId)
    parent.ask(FindNodeForStoringData(data, _))
  }

  /**
   * This function "asks" the parent actor to get all movies from a node in the ring
   */
  def getAllMovies(entityId: String): Future[AllData] = {
    val parent = ClusterSharding(system).entityRefFor(Parent.typeKey, entityId)
    parent.ask(GetAllData)
  }

  /**
   * This function "asks" the parent node to find the node in the ring storing the requested movie
   * @param name Name of the movie
   */
  def getMovie(name: String, entityId: String): Future[DataActionResponse] = {
    val parent = ClusterSharding(system).entityRefFor(Parent.typeKey, entityId)
    parent.ask(FindSuccessorToFindData(name, _))
  }

  val userRoutes: Route =
    pathPrefix("movies" / LongNumber) {entityId =>
      concat(
        pathEnd {
          post {
            entity(as[Data]) { movie =>
              onSuccess(createMovie(movie, entityId.toString)) { response =>
                complete((StatusCodes.Created, response.asInstanceOf[DataStorageResponseSuccess]))
              }
            }
          }
        },

        pathPrefix("getMovie") {
          path(Segment) { movieName =>
            get {
              onSuccess(getMovie(movieName, entityId.toString)) { response => {
                //noinspection TypeCheckCanBeMatch
                if (response.isInstanceOf[DataResponseSuccess])
                  complete(response.asInstanceOf[DataResponseSuccess])
                else
                  complete(response.asInstanceOf[DataResponseFailed])
              }
              }
            }
          }
        },

        pathEnd {
          get {
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<center><h1>Hello! Please upload/download some movies</h1></center>"))
          }
        }
      )
    }
}
