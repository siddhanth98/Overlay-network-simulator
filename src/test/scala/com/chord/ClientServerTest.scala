package com.chord

import java.io.File

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import com.chord.Aggregator.Aggregate
import com.chord.HttpClient.{FinishSimulation, GetMovie, PostMovie}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.{Logger, LoggerFactory}

class ClientServerTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  val logger: Logger = LoggerFactory.getLogger(classOf[ChordNodeTest])
  val config: Config =
    ConfigFactory.parseFile(new File("src/main/resources/configuration/test.conf"))
  val m: Int = config.getInt("app.NUMBER_OF_FINGERS")
  val n: Int = config.getInt("app.NUMBER_OF_NODES")
  val dumpPeriod: Int = config.getInt("app.DUMP_PERIOD_IN_SECONDS")
  val numberOfClients: Int = config.getInt("app.NUMBER_OF_CLIENTS")
  val movie1Name: String = config.getString("app.MOVIE1.MOVIE_NAME")
  val movie1Size: Int = config.getInt("app.MOVIE1.MOVIE_SIZE")
  val movie1Genre: String = config.getString("app.MOVIE1.MOVIE_GENRE")
  val movie2Name: String = config.getString("app.MOVIE2.MOVIE_NAME")
  val movie2Size: Int = config.getInt("app.MOVIE2.MOVIE_SIZE")
  val movie2Genre: String = config.getString("app.MOVIE2.MOVIE_GENRE")
  val aggregatorTestProbe: TestProbe[Aggregate] = createTestProbe[Aggregate]()
  val client: ActorRef[HttpClient.Command] = spawn(HttpClient(aggregatorTestProbe.ref), "TestClient")

  override def beforeAll(): Unit = {
    HttpServer(m, n, dumpPeriod)
    Thread.sleep(1000)
    client ! PostMovie(movie1Name, movie1Size, movie1Genre)
    client ! PostMovie(movie2Name, movie2Size, movie2Genre)
    Thread.sleep(2000)
  }

  "Chord server" must {
    "always find a movie if it has been uploaded" in {
      val newClient = spawn(HttpClient(aggregatorTestProbe.ref), "TestClient2")
      newClient ! GetMovie(movie1Name)
      Thread.sleep(3000)
      newClient ! FinishSimulation
      Thread.sleep(2000)
      aggregatorTestProbe.expectMessage(Aggregate(newClient, 1, 0))
    }

    "respond with a not-found message if a non-existent movie is queried" in {
      val newClient = spawn(HttpClient(aggregatorTestProbe.ref), "TestClient3")
      newClient ! GetMovie(movie1Name+movie2Name)
      Thread.sleep(3000)
      newClient ! FinishSimulation
      Thread.sleep(2000)
      aggregatorTestProbe.expectMessage(Aggregate(newClient, 0, 1))
    }
  }

  "Aggregator" must {
    "get an aggregate message from client's counter having success count 2 and fail count 0" in {
      val newClient = spawn(HttpClient(aggregatorTestProbe.ref), "TestClient4")
      newClient ! GetMovie(movie1Name)
      newClient ! GetMovie(movie2Name)
      Thread.sleep(3000)
      newClient ! FinishSimulation
      Thread.sleep(2000)
      aggregatorTestProbe.expectMessage(Aggregate(newClient, 2, 0))
    }
  }
}
