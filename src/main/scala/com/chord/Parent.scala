package com.chord

import java.io.{File, FileWriter}
import java.nio.ByteBuffer
import java.security.MessageDigest
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.utils.UnsignedInt

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.math.BigInt.javaBigInteger2bigInt

/**
 * This actor is responsible for spawning child chord ring nodes and routing data request to a random node in the ring
 * It maintains a timer whose ticks determine the period at which the state of all ring nodes is to be dumped on disk
 * in yaml format
 * This actor also maintains a map of (ring slot hash -> actor reference) to find the immediate successor and predecessor of a
 * newly spawned node in the ring.
 */
object Parent {

  trait Command
  final case class Join(successorNodeRef: ActorRef[Node.Command], predecessorNodeRef: ActorRef[Node.Command],
                        nextSuccessorNodeRef: ActorRef[Node.Command], nextSuccessorHashValue: Int, hashValue: Int)
    extends Command with Node.Command
  final case object ObserveFingerTable extends Command
  final case class ActorName(name: String) extends Command
  final case class DumpActorState(replyTo: ActorRef[Command]) extends Command with Node.Command
  final case object TerminateOrJoinNode extends Command

  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)

  def getSignedHash(m: Int, s: String): Int = (UnsignedInt(ByteBuffer.wrap(md5(s)).getInt).bigIntegerValue % Math.pow(2, m).toInt).intValue()

  def apply(m: Int, n: Int, dumpPeriod: Int, nodeJoinFailurePeriod: Int): Behavior[Command] =
    Behaviors.setup[Command] (context => Behaviors.withTimers { timer =>
      val slotToAddress = spawnServers(m, n, context)
      timer.startTimerAtFixedRate(DumpActorState(context.self), dumpPeriod.seconds)
      timer.startTimerAtFixedRate(TerminateOrJoinNode, nodeJoinFailurePeriod.seconds)
      context.log.info(s"${context.self.path}\t:\tSpawned actor hashes => [${slotToAddress.keySet.toList.mkString(", ")}]")
      update(m, n, slotToAddress, slotToAddress.keySet.toList, List.empty, nodeJoinFlag=true)
    })

  /**
   * This function determines the behavior of this parent actor
   * @param m Number of finger table entries of each actor node
   * @param n Maximum number of nodes in the ring
   * @param slotToAddress map of ring slot hash to actor ref
   * @param actorHashesList List of all actor node hash values currently in the
   * @param actorStates List of actor states to dump
   */
  def update(m: Int, n: Int, slotToAddress: mutable.Map[Int, ActorRef[Node.Command]], actorHashesList: List[Int],
             actorStates: List[Node.StateToDump], nodeJoinFlag: Boolean): Behavior[Command] =
      Behaviors.receive {

          /*
          * This message is sent by the parent actor to itself to either terminate / create a random node in the chord ring
          */
        case (context, TerminateOrJoinNode) =>
          if (nodeJoinFlag) {
            context.log.info(s"${context.self.path}\t:\tCreating new node in the chord ring...")
            val newSlotToAddress = spawnNewNode(slotToAddress, context, m)
            update(m, n, newSlotToAddress, newSlotToAddress.keySet.toList, actorStates, nodeJoinFlag=false)
          }
          else {
            context.log.info(s"${context.self.path}\t:\tStopping a random node in the chord ring")
            val newSlotToAddress = terminateNode(slotToAddress, actorHashesList, context, m)
            update(m, n, newSlotToAddress, newSlotToAddress.keySet.toList, actorStates, nodeJoinFlag=true)
          }

            /*
            * Parent actor has sent itself a message to collect and dump all actor states
            */
        case (context, DumpActorState(replyTo)) =>
          context.log.info(s"${context.self.path}\t:\tReceived dump message. Telling all nodes to send their states to me.")
          slotToAddress.values.foreach(actor => actor ! DumpActorState(replyTo))
          Behaviors.same

          /*
          * Parent actor has received state from one of the actor nodes. If states from all actors have been received,
          * then this actor will dump all collected states on disk.
          * Otherwise it will continue to wait for states from other actors
          */
        case (context, Node.ActorState(actorRef, state)) =>
          context.log.info(s"${context.self.path}\t:\tGot dump state from actor $actorRef")
          if (actorStates.size == slotToAddress.keySet.size-1) {
            dumpState(state :: actorStates)
            context.log.info(s"${context.self.path}\t:\tFinished dumping all actor states")
            update(m, n, slotToAddress, actorHashesList, List.empty, nodeJoinFlag)
          }
          else update(m, n, slotToAddress, actorHashesList, state :: actorStates, nodeJoinFlag)

        case (context, Node.DataStorageResponseSuccess(d)) =>
          context.log.info(s"${context.self.path}\t:\tGot response $d")
          Behaviors.same

        case (context, Node.DataResponseSuccess(data)) =>
          context.log.info(s"${context.self.path}\t:\tdata found is:\t${data.get}")
          Behaviors.same

        case (context, Node.DataResponseFailed(d)) =>
          context.log.info(s"${context.self.path}\t:\tgot response {$d} ")
          Behaviors.same

          /*
          * This actor has received a request for storing some data(movie). It will find a random node in the ring and forward
          * the request to it.
          */
        case (context, Node.FindNodeForStoringData(data, srcRouteRef)) =>
          context.log.info(s"${context.self.path}\t:\tGot request for finding a node to store data ${data.name}")
          val randomActorToQuery = findRandomActor(slotToAddress, actorHashesList, context)
          context.log.info(s"${context.self.path}\t:\tForwarding data storage request to node $randomActorToQuery")
          randomActorToQuery ! Node.FindNodeForStoringData(data, srcRouteRef)
          Behaviors.same

          /*
          * This actor has received a request for finding a movie. It will find a random node in the ring and forward the
          * request to it.
          */
        case (context, Node.FindSuccessorToFindData(name, srcRouteRef)) =>
          context.log.info(s"${context.self.path}\t:\tGot request to find data $name")
          val randomActorToQuery = findRandomActor(slotToAddress, actorHashesList, context)
          context.log.info(s"${context.self.path}\t:\tForwarding data query request to node $randomActorToQuery")
          randomActorToQuery ! Node.FindSuccessorToFindData(name, srcRouteRef)
          Behaviors.same

          /*
          * This actor has received a request to get all movies. It will randomly select a node and forward the request
          * to it.
          */
        case (context, Node.GetAllData(replyTo)) =>
          context.log.info(s"${context.self.path}\t:\tGot request to find all data")
          val randomActorToQuery = findRandomActor(slotToAddress, actorHashesList, context)
          context.log.info(s"${context.self.path}\t:\tForwarding request to get all data to node $randomActorToQuery")
          randomActorToQuery ! Node.GetAllData(replyTo)
          Behaviors.same
      }

  /**
   * This function will spawn all chord nodes in the ring
   * @param m The number of finger table entries
   * @param n The maximum number of nodes in the ring
   * @param context Execution context of this actor
   */
  def spawnServers(m: Int, n: Int, context: ActorContext[Command]): mutable.Map[Int, ActorRef[Node.Command]] = {
    val newSlotToAddress = mutable.Map[Int, ActorRef[Node.Command]]()

    (1 to n).foreach(_ => {
      val serverNode = context.spawn(Node(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Node.Command]](),
        null, null, null, -1), "Server" + scala.util.Random.nextInt(100000).toString)
      val serverNodeHash = getSignedHash(m, serverNode.path.toString)

      if (!newSlotToAddress.contains(serverNodeHash)) {
        if (newSlotToAddress.keySet.toList.isEmpty) {
          newSlotToAddress += (serverNodeHash -> serverNode)
          serverNode ! Join(serverNode,
            findPredecessor(serverNodeHash, newSlotToAddress, context),
            findNextSuccessor(serverNodeHash, newSlotToAddress),
            findNextSuccessorIndex(findNextSuccessorIndex(serverNodeHash, newSlotToAddress), newSlotToAddress),
            serverNodeHash)

          context.log.info(s"${context.self.path}\t:\tSpawned 1st server $serverNode having hash $serverNodeHash")
        }
        else {
          newSlotToAddress += (serverNodeHash -> serverNode)
          serverNode ! Join(findExistingSuccessorNode(serverNodeHash, newSlotToAddress, context),
            findPredecessor(serverNodeHash, newSlotToAddress, context),
            findNextSuccessor(serverNodeHash, newSlotToAddress),
            findNextSuccessorIndex(findNextSuccessorIndex(serverNodeHash, newSlotToAddress), newSlotToAddress),
            serverNodeHash)

          context.log.info(s"${context.self.path}\t:\tSpawned server $serverNode having hash $serverNodeHash")
        }
      }
      Thread.sleep(500)
    })
    newSlotToAddress
  }

  def spawnNewNode(slotToAddress: mutable.Map[Int, ActorRef[Node.Command]], context: ActorContext[Command],
                   m: Int):
  mutable.Map[Int, ActorRef[Node.Command]] = {
    val newNode = context.spawn(Node(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Node.Command]](), null, null, null, -1),
      s"Server${scala.util.Random.nextInt(100000)}")
    val newNodeHash = getSignedHash(m, newNode.path.toString)
    if (slotToAddress.contains(newNodeHash)) {
      context.stop(newNode)
      slotToAddress
    }
    else {
      context.log.info(s"${context.self.path}\t:\tSpawned new node $newNode with hash $newNodeHash")
      newNode ! Join(findExistingSuccessorNode(newNodeHash, slotToAddress, context),
        findPredecessor(newNodeHash, slotToAddress, context), findNextSuccessor(newNodeHash, slotToAddress),
        findNextSuccessorIndex(findNextSuccessorIndex(newNodeHash, slotToAddress), slotToAddress),
        newNodeHash)
      slotToAddress + (newNodeHash -> newNode)
    }
  }

  def terminateNode(slotToAddress: mutable.Map[Int, ActorRef[Node.Command]], actorHashesList: List[Int], context: ActorContext[Command],
                    m: Int): mutable.Map[Int, ActorRef[Node.Command]] = {
    val randomNode = findRandomActor(slotToAddress, actorHashesList, context)
    val randomNodeHash = getSignedHash(m, randomNode.path.toString)
    context.stop(randomNode)
    slotToAddress-randomNodeHash
  }

  /**
   * Use the scala.util.Random class to randomly select a chord node in the ring.
   * @param slotToAddress map (ring slot hash to actor ref)
   * @param actorHashesList List of actor node hashes
   * @param context actor execution context
   */
  def findRandomActor(slotToAddress: mutable.Map[Int, ActorRef[Node.Command]], actorHashesList: List[Int],
                      context: ActorContext[Command]): ActorRef[Node.Command] = {
    val randomIndex = scala.util.Random.nextInt(actorHashesList.size)
    context.log.info(s"${context.self.path}\t:\tRandomly generated hash is ${actorHashesList(randomIndex)}")
    slotToAddress(actorHashesList(randomIndex))
  }

  /**
   * Cyclically go through all entries and find the predecessor node of the new node joining
   */
  def findPredecessor(n: Int, slotToAddress: mutable.Map[Int, ActorRef[Node.Command]],
                      context: ActorContext[Command]): ActorRef[Node.Command] = {
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
  def findExistingSuccessorNode(n: Int, slotToAddress: mutable.Map[Int, ActorRef[Node.Command]],
                                context: ActorContext[Command]): ActorRef[Node.Command] = {
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

  def findNextSuccessor(n: Int, slotToAddress: mutable.Map[Int, ActorRef[Node.Command]]): ActorRef[Node.Command] =
    slotToAddress(findNextSuccessorIndex(findNextSuccessorIndex(n, slotToAddress), slotToAddress))

  def findNextSuccessorIndex(n: Int, slotToAddress: mutable.Map[Int, ActorRef[Node.Command]]): Int = {
    val resultIndex = slotToAddress.keySet.toList.sorted.find(_ > n)
    if (resultIndex.isEmpty) slotToAddress.keySet.toList.sorted.find(_ <= n).get
    else resultIndex.get
  }

  /**
   * This function will write collected states from all actors to a yaml file on disk
   * @param dumps The list of actor states collected
   */
  def dumpState(dumps: List[Node.StateToDump]): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/chordState.yml"))
    val objectMapper = new ObjectMapper(new YAMLFactory().enable(Feature.INDENT_ARRAYS))
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValue(outputFile, dumps)
    outputFile.close()
  }
}
