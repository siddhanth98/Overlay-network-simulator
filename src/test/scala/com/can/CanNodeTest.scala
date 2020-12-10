package com.can

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.can.Node.{GetCoordinates, NodeCoordinates}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.{Logger, LoggerFactory}

import java.io.File

class CanNodeTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  val logger: Logger = LoggerFactory.getLogger(classOf[CanNodeTest])
  val config: Config =
    ConfigFactory.parseFile(new File("src/main/resources/configuration/test.conf"))
  val replicationPeriod: Int = config.getInt("app.REPLICATION_PERIOD")
  val endX: Int = config.getInt("app.CAN.END_X")
  val endY: Int = config.getInt("app.CAN.END_Y")

  "First CAN node" must {
    "span all of the CAN coordinate system" in {
      val nodeProbe = createTestProbe[NodeCoordinates]()
      val node = spawn(Node(0, endX, endY, replicationPeriod))

      node ! GetCoordinates(nodeProbe.ref)
      val nodeCoordinates = nodeProbe.receiveMessage().coordinates

      logger.info(s"1st node in the CAN (${endX}X${endY}) system spans coordinates : [(${nodeCoordinates(0)(0)}, ${nodeCoordinates(0)(1)}), (${nodeCoordinates(1)(0)}, ${nodeCoordinates(1)(1)})]")

      nodeCoordinates(0)(0) should === (1)
      nodeCoordinates(0)(1) should === (endX)
      nodeCoordinates(1)(0) should === (1)
      nodeCoordinates(1)(1) should === (endY)
    }
  }

  "Second CAN node" must {
    "acquire only half of the 1st node's zone" in {
      val nodeProbe = createTestProbe[NodeCoordinates]()

      val node1 = spawn(Node(0, endX, endY, replicationPeriod))
      Thread.sleep(500)
      val node2 = spawn(Node(1, node1, 'X', replicationPeriod))

      Thread.sleep(500)

      node1 ! GetCoordinates(nodeProbe.ref)
      val node1Coordinates = nodeProbe.receiveMessage().coordinates

      Thread.sleep(500)

      node2 ! GetCoordinates(nodeProbe.ref)
      val node2Coordinates = nodeProbe.receiveMessage().coordinates

      logger.info(s"1st node in the CAN (${endX}X${endY}) system spans coordinates : " +
        s"[(${node1Coordinates(0)(0)}, ${node1Coordinates(0)(1)}), (${node1Coordinates(1)(0)}, ${node1Coordinates(1)(1)})]")

      logger.info(s"2st node in the CAN (${endX}X${endY}) system spans coordinates : " +
        s"[(${node2Coordinates(0)(0)}, ${node2Coordinates(0)(1)}), (${node2Coordinates(1)(0)}, ${node2Coordinates(1)(1)})]")

      node1Coordinates(0)(0) should === (1)
      node1Coordinates(0)(1) should === ((1+endX)/2D)
      node1Coordinates(1)(0) should === (1)
      node1Coordinates(1)(1) should === (endY)

      node2Coordinates(0)(0) should === ((1+endX)/2D)
      node2Coordinates(0)(1) should === (endX)
      node2Coordinates(1)(0) should === (1)
      node2Coordinates(1)(1) should === (endY)
    }
  }
}
