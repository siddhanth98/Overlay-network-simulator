package com.chord

import akka.actor.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse}
import ch.qos.logback.classic.util.ContextInitializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

object HttpClient {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/logback.xml")

  trait Command
  final case class PostMovie(name: String, size: Int, genre: String) extends Command
  final case class GetMovie(name: String) extends Command
  implicit val system: ActorSystem = ActorSystem()
  implicit val dispatcher: ExecutionContextExecutor = system.dispatcher

  final val logger: Logger = LoggerFactory.getLogger(HttpClient.getClass)

  def apply(): Behavior[Command] =
    Behaviors.receive {
      case (context, PostMovie(name, size, genre)) =>
        context.log.info(s"${context.self.path}\t:\tSending post request for uploading movie => (name='$name', size=$size, genre='$genre')")
        sendRequest(context, makeHttpPostRequest(name, size, genre))
          .foreach(res => logger.info(s"${context.self.path}\t:\tgot POST response => ($res)"))
        Behaviors.same

      case (context, GetMovie(name)) =>
        context.log.info(s"${context.self.path}\t:\tSending get request for obtaining movie => (name=$name)")
        sendRequest(context, makeHttpGetRequest(name))
          .foreach(res => logger.info(s"${context.self.path}\t:\tgot GET response => ($res)"))
        Behaviors.same
    }

  def makeHttpPostRequest(name: String, size: Int, genre: String): HttpRequest =
    HttpRequest (
      method = HttpMethods.POST,
      uri = "http://localhost:8080/movies",
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"name": "$name", "size": $size, "genre": "$genre"}"""
      )
    )

  def makeHttpGetRequest(name: String): HttpRequest =
    HttpRequest (
      method = HttpMethods.GET,
      uri = s"http://localhost:8080/movies/getMovie/$name"
    )

  def sendRequest(context: ActorContext[Command], request: HttpRequest): Future[String] = {
    val responseFuture: Future[HttpResponse] = Http()(context.system).singleRequest(request)
    val entityFuture: Future[HttpEntity.Strict] = responseFuture.flatMap(m => m.entity.toStrict(5.seconds))
    entityFuture.map(m => m.data.utf8String)
  }
}
