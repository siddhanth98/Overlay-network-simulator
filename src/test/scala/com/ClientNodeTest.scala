package com

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import com.chord.ChordNodeTest
import com.client.HttpClient
import com.client.HttpClient.{FinishSimulation, GetMovie, PostMovie}
import com.server.HttpServer
import com.simulation.Aggregator.Aggregate
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.{Logger, LoggerFactory}

import java.io.File

/**
 * This class is responsible for testing the client server request response cycle of the simulation.
 */
class ClientNodeTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  val logger: Logger = LoggerFactory.getLogger(classOf[ChordNodeTest])
  val config: Config =
    ConfigFactory.parseFile(new File("src/main/resources/configuration/test.conf"))
  val numberOfClients: Int = config.getInt("app.NUMBER_OF_CLIENTS")
  val movie1Name: String = config.getString("app.MOVIE1.MOVIE_NAME")
  val movie1Size: Int = config.getInt("app.MOVIE1.MOVIE_SIZE")
  val movie1Genre: String = config.getString("app.MOVIE1.MOVIE_GENRE")
  val movie2Name: String = config.getString("app.MOVIE2.MOVIE_NAME")
  val movie2Size: Int = config.getInt("app.MOVIE2.MOVIE_SIZE")
  val movie2Genre: String = config.getString("app.MOVIE2.MOVIE_GENRE")
  val aggregatorTestProbe: TestProbe[Aggregate] = createTestProbe[Aggregate]()
  val client: ActorRef[HttpClient.Command] = spawn(com.client.HttpClient(aggregatorTestProbe.ref), "TestClient")

  /**
   * Before all tests are run, start the http server and post 2 movies in it to be tested.
   */
  override def beforeAll(): Unit = {
    HttpServer(config)
    Thread.sleep(1000)
    client ! PostMovie(movie1Name, movie1Size, movie1Genre)
    Thread.sleep(2000)
    client ! PostMovie(movie2Name, movie2Size, movie2Genre)
    Thread.sleep(5000)
  }

  "Chord server" must {
    "always find a movie if it has been uploaded" in {
      val newClient = spawn(com.client.HttpClient(aggregatorTestProbe.ref), "TestClient2")
      newClient ! GetMovie(movie1Name)
      Thread.sleep(3000)
      newClient ! FinishSimulation
      Thread.sleep(2000)
      aggregatorTestProbe.expectMessage(Aggregate(newClient, 1, 0))
    }

    "respond with a not-found message if a non-existent movie is queried" in {
      val newClient = spawn(com.client.HttpClient(aggregatorTestProbe.ref), "TestClient3")
      newClient ! GetMovie(movie1Name+movie2Name)
      Thread.sleep(3000)
      newClient ! FinishSimulation
      Thread.sleep(2000)
      aggregatorTestProbe.expectMessage(Aggregate(newClient, 0, 1))
    }
  }

  "Aggregator" must {
    "get an aggregate message from client's counter having success count 2 and fail count 0 " +
      "after 2 existing movies are queried and simulation ends" in {
      val newClient = spawn(com.client.HttpClient(aggregatorTestProbe.ref), "TestClient4")
      newClient ! GetMovie(movie1Name)
      newClient ! GetMovie(movie2Name)
      Thread.sleep(3000)
      newClient ! FinishSimulation
      Thread.sleep(2000)
      aggregatorTestProbe.expectMessage(Aggregate(newClient, 2, 0))
    }
  }
}
