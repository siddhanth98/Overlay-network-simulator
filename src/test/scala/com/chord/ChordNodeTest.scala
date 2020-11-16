package com.chord

import java.io.File
import java.nio.ByteBuffer
import java.security.MessageDigest

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import com.chord.Parent.Join
import com.chord.Server.{ActorStateResponse, Command, GetActorState}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.mutable
import scala.math.BigInt.javaBigInteger2bigInt

class ChordNodeTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  val config: Config =
    ConfigFactory.parseFile(new File("src/main/resources/configuration/test.conf"))
  val m: Int = config.getInt("app.NUMBER_OF_FINGERS")

  "First Chord Node" must {
    "have all finger table entries reference itself" in {
      val nodeProbe = createTestProbe[ActorStateResponse]()
      val node = spawn(Server(m, mutable.Map.empty, mutable.Map.empty, null, null), "MyChordNode")
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
      val node1Hash = getSignedHash(m, node1.path.toString)
      val node2Hash = getSignedHash(m, node2.path.toString)

      node1 ! Join(node1, node1)
      node2 ! Join(node1, node1)

      val expectedSlotToHashNode1 = mutable.Map[Int, Int]()
      val expectedHashToRefNode1 = mutable.Map[Int, ActorRef[Command]]()
      val expectedSlotToHashNode2 = mutable.Map[Int, Int]()
      val expectedHashtoRefNode2 = mutable.Map[Int, ActorRef[Command]]()

      (1 to m).foreach(i => expectedSlotToHashNode1 += (i-1 -> node1Hash))
      expectedHashToRefNode1 += (node1Hash -> node1)

      nodeProbe.expectMessage(ActorStateResponse(expectedSlotToHashNode1, expectedHashToRefNode1, node1Hash, node2))
      /*val node2State = nodeProbe.receiveMessage()

      node1State.predecessor should === (node2)
      node2State.predecessor should === (node1)
      node1State.hashToRef(node1State.slotToHash(0)) should === (node2)
      node2State.hashToRef(node2State.slotToHash(0)) should === (node1)*/
    }
  }

  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)
  def getSignedHash(m: Int, s: String): Int = (UnsignedInt(ByteBuffer.wrap(md5(s)).getInt).bigIntegerValue % Math.pow(2, m).toInt).intValue()
}
