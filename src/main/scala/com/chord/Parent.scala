package com.chord

import java.nio.ByteBuffer
import java.security.MessageDigest

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}

import scala.collection.mutable
import scala.math.BigInt.javaBigInteger2bigInt

object Parent {

  trait Command
  final case class Join(successorNodeRef: ActorRef[Server.Command], predecessorNodeRef: ActorRef[Server.Command]) extends Command with Server.Command
  final case object ObserveFingerTable extends Command
  final case class ActorName(name: String) extends Command

  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)

  def getSignedHash(m: Int, s: String): Int = (UnsignedInt(ByteBuffer.wrap(md5(s)).getInt).bigIntegerValue % Math.pow(2, m).toInt).intValue()

  def apply(m: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]]): Behavior[Command] =
    Behaviors.setup[Command] (context => {
      val serverNode1 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
        null, null), "Server" + scala.util.Random.nextInt(100000).toString)
      val serverNode1Hash = getSignedHash(m, serverNode1.path.toString)
      slotToAddress += (serverNode1Hash -> serverNode1)
      serverNode1 ! Join(serverNode1, findPredecessor(m, serverNode1Hash, slotToAddress, context))

      val serverNode2 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
      null, null), "Server" + scala.util.Random.nextInt(100000).toString)
      val serverNode2Hash = getSignedHash(m, serverNode2.path.toString)
      slotToAddress += (serverNode2Hash -> serverNode2)
      serverNode2 ! Join(findExistingSuccessorNode(m, serverNode2Hash, slotToAddress, context), findPredecessor(m, serverNode2Hash, slotToAddress, context))

      Thread.sleep(2000)

      val serverNode3 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
        null, null), "Server" + scala.util.Random.nextInt(100000).toString)
      val serverNode3Hash = getSignedHash(m, serverNode3.path.toString)
      slotToAddress += (serverNode3Hash -> serverNode3)
      serverNode3 ! Join(findExistingSuccessorNode(m, serverNode3Hash, slotToAddress, context), findPredecessor(m, serverNode3Hash, slotToAddress, context))

      Thread.sleep(2000)
      val serverNode4 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
        null, null), "Server" + scala.util.Random.nextInt(100000).toString)
      val serverNode4Hash = getSignedHash(m, serverNode4.path.toString)
      slotToAddress += (serverNode4Hash -> serverNode4)
      serverNode4 ! Join(findExistingSuccessorNode(m, serverNode4Hash, slotToAddress, context), findPredecessor(m, serverNode4Hash, slotToAddress, context))
      Behaviors.same
    })

  /**
   * Cyclically go through all entries and find the predecessor node of the new node joining
   */
  def findPredecessor(m: Int, n: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]],
                     context: ActorContext[Command]): ActorRef[Server.Command] = {
    val resultIndex = slotToAddress.keySet.toList.sorted.reverse.find(_ < n)
    if (resultIndex.isEmpty) {
      context.log.info(s"${context.self.path}\t:\tpredecessor node of $n = ${slotToAddress.keySet.toList.sorted.reverse.find(_ >= n).get}")
      slotToAddress(slotToAddress.keySet.toList.sorted.reverse.find(_ >= n).get)
    }
    else {
      context.log.info(s"${context.self.path}\t:\tpredecessor node of $n = ${slotToAddress(resultIndex.get)}")
      slotToAddress(resultIndex.get)
    }
  }

  /**
   * Cyclically go through all entries in the slotToAddress mapping table and find the first successor node of new node n
   */
  def findExistingSuccessorNode(m: Int, n: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]],
                               context: ActorContext[Command]): ActorRef[Server.Command] = {
    val resultIndex = slotToAddress.keySet.toList.sorted.find(_ > n)
    if (resultIndex.isEmpty) {
      context.log.info(s"${context.self.path}\t:\tsuccessor node of $n = ${slotToAddress(slotToAddress.keySet.toList.sorted.find(_ <= n).get)}")
      slotToAddress(slotToAddress.keySet.toList.sorted.find(_ <= n).get)
    }
    else {
      context.log.info(s"${context.self.path}\t:\tsuccessor node of $n = ${slotToAddress(resultIndex.get)}")
      slotToAddress(resultIndex.get)
    }
  }
}
