package com.chord

import java.nio.ByteBuffer
import java.security.MessageDigest

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.chord.Server.FindSuccessorToFindData

import scala.collection.mutable
import scala.math.BigInt.javaBigInteger2bigInt

object Parent {

  trait Command
  final case class Join(successorNodeRef: ActorRef[Server.Command], predecessorNodeRef: ActorRef[Server.Command]) extends Command with Server.Command
  final case object ObserveFingerTable extends Command
  final case class ActorName(name: String) extends Command
  final case class ActorList(replyTo: ActorRef[ActorListResponse]) extends Command
  final case class ActorListResponse(actorList: List[ActorRef[Server.Command]]) extends Command

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

      val actorHashesList = slotToAddress.keySet.toList
      Thread.sleep(2000)

      val serverNode3 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
        null, null), "Server" + scala.util.Random.nextInt(100000).toString)
      val serverNode3Hash = getSignedHash(m, serverNode3.path.toString)
      slotToAddress += (serverNode3Hash -> serverNode3)
      serverNode3 ! Join(findExistingSuccessorNode(m, serverNode3Hash, slotToAddress, context), findPredecessor(m, serverNode3Hash, slotToAddress, context))

      /*Thread.sleep(2000)
      val serverNode4 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
        null, null), "Server" + scala.util.Random.nextInt(100000).toString)
      val serverNode4Hash = getSignedHash(m, serverNode4.path.toString)
      slotToAddress += (serverNode4Hash -> serverNode4)
      serverNode4 ! Join(findExistingSuccessorNode(m, serverNode4Hash, slotToAddress, context), findPredecessor(m, serverNode4Hash, slotToAddress, context))

      Thread.sleep(2000)
      val dataName = s"Movie${scala.util.Random.nextInt(1000)}"
      val dataHash = getSignedHash(m, dataName)
      val data = Server.Data(dataName, 500, s"Action")
      context.log.info(s"${context.self.path}\t:\tSending data key $dataHash to be stored")
      serverNode3 ! Server.FindNodeForStoringData(data, context.self)

      Thread.sleep(2000)
      val dataName2 = s"Movie${scala.util.Random.nextInt(1000)}"
      val dataHash2 = getSignedHash(m, dataName2)
      val data2 = Server.Data(dataName2, 501, s"Comedy")

      context.log.info(s"${context.self.path}\t:\tSending data key $dataHash2 to be stored")
      serverNode1 ! Server.FindNodeForStoringData(data2, context.self)

      serverNode4 ! FindSuccessorToFindData(dataName2, context.self)*/

      Behaviors.receive {
        case (context, Server.DataStorageResponseSuccess(d)) =>
          context.log.info(s"${context.self.path}\t:\tGot response $d")
          Behaviors.same

        case (context, Server.DataResponseSuccess(data)) =>
          context.log.info(s"${context.self.path}\t:\tdata found is:\t${data.get}")
          Behaviors.same

        case (context, Server.DataResponseFailed(d)) =>
          context.log.info(s"${context.self.path}\t:\tgot response {$d} ")
          Behaviors.same

        case (context, ActorList(replyTo)) =>
          context.log.info(s"${context.self.path}\t:\tGot request from guardian to get actor list")
          replyTo ! ActorListResponse(List(serverNode1, serverNode2))
          Behaviors.same

        case x @ (context, Server.FindNodeForStoringData(data, srcRouteRef)) =>
          context.log.info(s"${context.self.path}\t:\tGot request for finding a node to store data ${data.name}")
          val randomActorToQuery = findRandomActor(slotToAddress, actorHashesList, context)
          context.log.info(s"${context.self.path}\t:\tForwarding data storage request to node $randomActorToQuery")
          randomActorToQuery ! Server.FindNodeForStoringData(data, srcRouteRef)
          Behaviors.same

        case x @ (context, Server.FindSuccessorToFindData(name, srcRouteRef)) =>
          context.log.info(s"${context.self.path}\t:\tGot request to find data $name")
          val randomActorToQuery = findRandomActor(slotToAddress, actorHashesList, context)
          context.log.info(s"${context.self.path}\t:\tForwarding data query request to node $randomActorToQuery")
          randomActorToQuery ! Server.FindSuccessorToFindData(name, srcRouteRef)
          Behaviors.same

        case x @ (context, Server.GetAllData(replyTo)) =>
          context.log.info(s"${context.self.path}\t:\tGot request to find all data")
          val randomActorToQuery = findRandomActor(slotToAddress, actorHashesList, context)
          context.log.info(s"${context.self.path}\t:\tForwarding request to get all data to node $randomActorToQuery")
          randomActorToQuery ! Server.GetAllData(replyTo)
          Behaviors.same
      }
    })

  def findRandomActor(slotToAddress: mutable.Map[Int, ActorRef[Server.Command]], actorHashesList: List[Int],
                     context: ActorContext[Command]): ActorRef[Server.Command] = {
    val randomIndex = scala.util.Random.nextInt(actorHashesList.size)
    context.log.info(s"${context.self.path}\t:\tRandomly generated hash is ${actorHashesList(randomIndex)}")
    slotToAddress(actorHashesList(randomIndex))
  }

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
