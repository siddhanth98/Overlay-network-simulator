package com.client

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import ch.qos.logback.classic.util.ContextInitializer
import com.client
import com.simulation.Aggregator
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * This actor makes http requests at http://localhost:8000/movies to post/get movies
 * The type of request to be made is determined by the simulation actor
 * This actor creates a child actor named Counter which tracks the number of successful(movies found) and failed(movies not found)
 * requests made by this actor instance.
 */
object HttpClient {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/logback.xml")

  trait Command
  final case class PostMovie(name: String, size: Int, genre: String) extends Command
  final case class GetMovie(name: String) extends Command
  final case object FinishSimulation extends Command
  final case object FinishCounter extends Command
  implicit val system: ActorSystem = ActorSystem()
  implicit val dispatcher: ExecutionContextExecutor = system.dispatcher

  final val logger: Logger = LoggerFactory.getLogger(HttpClient.getClass)

  def apply(aggregator: ActorRef[Aggregator.Aggregate]): Behavior[Command] = Behaviors.setup{context =>
    context.log.info(s"${context.self.path}\t:\tI was just created by the simulation. Creating a counter for me...")
    val counterActor = context.spawn(client.Counter(context.self, aggregator), "myCounter")
    process(counterActive=true, counterActor)
  }

  /**
   * This method defines the behavior of the client actor.
   * @param counterActive A flag which tells whether or not the client's counter actor is still active
   *                      to avoid sending Success/Failed messages to the counter actor after simulation ends and
   *                      the counter stops.
   * @param counterActor The counter actor reference
   */
  def process(counterActive: Boolean, counterActor: ActorRef[Counter.Command]): Behavior[Command] =
    Behaviors.receive {

      /*
       * The client has been requested by the simulation to make a post request to store a movie in the chord server
       */
      case (context, PostMovie(name, size, genre)) =>
        context.log.info(s"${context.self.path}\t:\tSending post request for uploading movie => (name='$name', size=$size, genre='$genre')")
        sendRequest(context, makeHttpPostRequest(name, size, genre))
        Behaviors.same

      /*
       * The client has been requested to make a get request to get a random movie, which may or may not be stored previously
       */
      case (context, GetMovie(name)) =>
        context.log.info(s"${context.self.path}\t:\tSending get request for obtaining movie => (name=$name)")
        sendRequest(context, makeHttpGetRequest(name))
          .foreach(res => {
            if (res.contains("404") || res.contains("error")) {
              logger.info(s"${context.self.path}\t:\tMovie $name could not be found")
              counterActor ! Counter.Failure
            }
            else {
              logger.info(s"${context.self.path}\t:\tMovie $name successfully found - $res")
              counterActor ! Counter.Success
            }
          })
        Behaviors.same

      /*
       * The client is notified that the simulation has ended. Then the client sends a finish message to the counter
       * so that it stops.
       */
      case (context, FinishSimulation) =>
        context.log.info(s"${context.self.path}\t:\tSimulation has finished. Dumping counter state and shutting myself down...")
        counterActor ! Counter.Finish
        process(counterActive=false, counterActor)

      /*
       * The client is notified that the counter has finished sending the success and fail counter to the aggregator actor
       */
      case (context, FinishCounter) =>
        context.log.info(s"${context.self.path}\t:\tMy counter has dumped the state. Shutting myself down")
        Behaviors.stopped
    }

  /**
   * This function creates and returns a http post request object having the movie in the request body as a json
   * @param name Name of the movie
   * @param size Size of the movie in MB
   * @param genre Genre of the movie
   */
  def makeHttpPostRequest(name: String, size: Int, genre: String): HttpRequest =
    HttpRequest (
      method = HttpMethods.POST,
      uri = "http://localhost:8080/movies/1",
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"name": "$name", "size": $size, "genre": "$genre"}"""
      )
    )

  /**
   * This function creates and returns a http get request object
   * @param name Name of the movie to be queried
   */
  def makeHttpGetRequest(name: String): HttpRequest =
    HttpRequest (
      method = HttpMethods.GET,
      uri = s"http://localhost:8080/movies/1/getMovie/$name"
    )

  /**
   * This function makes the http request and returns a future which will be a response string sent back by the server
   * @param context Execution context of the client actor
   * @param request http post /get request object
   */
  def sendRequest(context: ActorContext[Command], request: HttpRequest): Future[String] = {
    val responseFuture: Future[HttpResponse] = Http()(context.system).singleRequest(request)
    val entityFuture: Future[HttpEntity.Strict] = responseFuture.flatMap(m => m.entity.toStrict(5.seconds))
    entityFuture.map(m => m.data.utf8String)
  }
}
