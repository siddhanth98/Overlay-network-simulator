package com.chord

import java.io.File
import java.nio.ByteBuffer
import java.security.MessageDigest

import akka.actor.testkit.typed.scaladsl.{LoggingTestKit, ScalaTestWithActorTestKit}
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import com.chord.HttpClient.{GetMovie, PostMovie}
import com.chord.Parent.Join
import com.chord.Server.{ActorStateResponse, AllPredecessorsUpdated, AllPredecessorsUpdatedResponse, Command, GetActorState}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.math.BigInt.javaBigInteger2bigInt

/**
 * This class is responsible for testing the finger tables and successor / predecessor pointers of
 * the chord nodes in the ring.
 */
class ChordNodeTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  val logger: Logger = LoggerFactory.getLogger(classOf[ChordNodeTest])
  val config: Config =
    ConfigFactory.parseFile(new File("src/main/resources/configuration/test.conf"))
  val m: Int = config.getInt("app.NUMBER_OF_FINGERS")

  "First Chord Node" must {
    "have all finger table entries reference itself" in {
      val nodeProbe = createTestProbe[ActorStateResponse]()
      val node = spawn(Server(m, mutable.Map.empty, mutable.Map.empty, null, null), "TestChordNode")
      val nodeHash = getSignedHash(m, node.path.toString)
      node ! Join(node, node)
      node ! GetActorState(nodeProbe.ref)

      val expectedSlotToHashMap = mutable.Map[Int, Int]()
      val expectedHashToRefMap = mutable.Map[Int, ActorRef[Command]]()
      (1 to m).foreach(i => expectedSlotToHashMap += (i - 1 -> nodeHash))
      expectedHashToRefMap += (nodeHash -> node)

      nodeProbe.expectMessage(ActorStateResponse(expectedSlotToHashMap, expectedHashToRefMap, nodeHash, node))
    }
  }

  "Each of two chord nodes" must {
    "reference each other as successor and predecessor" in {
      val nodeProbe = createTestProbe[ActorStateResponse]()

      val node1 = spawn(Server(m, mutable.Map.empty, mutable.Map.empty, null, null), "node1")
      val node2 = spawn(Server(m, mutable.Map.empty, mutable.Map.empty, null, null), "node2")

      node1 ! Join(node1, node1)
      node2 ! Join(node1, node1)

      Thread.sleep(500)

      node1 ! GetActorState(nodeProbe.ref)
      val node1State = nodeProbe.receiveMessage()
      node2 ! GetActorState(nodeProbe.ref)
      val node2State = nodeProbe.receiveMessage()

      logger.info(s"node1 successor is ${node1State.hashToRef(node1State.slotToHash(0))}")
      logger.info(s"node1 predecessor is ${node1State.predecessor}")

      logger.info(s"node2 successor is ${node2State.hashToRef(node2State.slotToHash(0))}")
      logger.info(s"node2 predecessor is ${node2State.predecessor}")

      node1State.predecessor should === (node2)
      node2State.predecessor should === (node1)
      node1State.hashToRef(node1State.slotToHash(0)) should === (node2)
      node2State.hashToRef(node2State.slotToHash(0)) should === (node1)
    }
  }

  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)
  def getSignedHash(m: Int, s: String): Int = (UnsignedInt(ByteBuffer.wrap(md5(s)).getInt).bigIntegerValue % Math.pow(2, m).toInt).intValue()
}
