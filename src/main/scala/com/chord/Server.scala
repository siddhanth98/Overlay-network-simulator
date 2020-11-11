package com.chord

import java.nio.ByteBuffer

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import java.security.MessageDigest

import scala.collection.mutable

object Server {
  /*val finger = new Array[ActorRef[Server.Command]](3)
  var successor: ActorRef[Server.Command] = _
  var predecessor: ActorRef[Server.Command] = _
  var hashValue: Int = _*/

  trait Command
  sealed trait DataActionResponse extends Command

  /**
   * Set of messages for querying and obtaining finger tables
   */
  final case class GetFingerTable(replyTo: ActorRef[FingerTableResponse]) extends Command
  final case class FingerTableResponse(slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]]) extends Command
  final case object FingerTableResponseError extends Command

  /**
   * Set of messages for querying and obtaining hash value of a node
   */
  final case class GetHashValue(replyTo: ActorRef[HashResponse]) extends Command
  final case class HashResponse(hashValue: Int) extends  Command
  final case object HashResponseError extends Command

  /**
   * Set of messages for querying and obtaining predecessor of a node
   */
  final case class GetPredecessor(replyTo: ActorRef[PredecessorResponse]) extends Command
  final case class PredecessorResponse(predecessor: ActorRef[Server.Command]) extends Command
  final case object PredecessorResponseError extends Command

  /**
   * Set of messages for querying and obtaining all relevant state properties from another server node
   */
  final case class GetActorState(replyTo: ActorRef[ActorStateResponse]) extends Command
  final case class ActorStateResponse(slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]],
                                     hashValue: Int, predecessor: ActorRef[Server.Command]) extends Command
  final case object ActorStateResponseError extends Command

  /**
   * Set of messages for representing data and data responses
   * Data here represents a movie type having a name, size in MB and genre
   */
  final case class Data(name: String, size: Int, genre: String)
  final case class GetData(id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class DataResponseSuccess(data: Option[Data]) extends DataActionResponse
  final case class DataResponseFailed(description: String) extends DataActionResponse

  /**
   * Set of messages for finding successors, predecessors and success and failure responses
   * when obtaining or storing data
   */
  final case class FindSuccessorToFindData(id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class FindPredecessorToFindData(id: Int, replyTo: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class SuccessorFoundForData(id: Int, successorRef: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class SuccessorNotFound(id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command

  /**
   * Set of messages for finding successors and predecessors when a new node joins
   */
  final case class FindSuccessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command]) extends Command
  final case class FindPredecessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command]) extends Command
  final case class SuccessorFound(i: Int, id: Int, successor: ActorRef[Command], successorHashValue: Int,
                                  newNodeRef: ActorRef[Command]) extends Command
  final case class NewSuccessorResponse(i: Int, successor: ActorRef[Command], successorHashValue: Int) extends Command
  final case object NewSuccessorResponseError extends Command

  /**
   * Set of messages to update finger tables of new node's predecessors
   */
  final case class UpdateFingerTable(newNode: ActorRef[Command], hashValue: Int, i: Int) extends Command
  final case class FindPredecessorToUpdate(i: Int, id: Int, replyTo: ActorRef[Server.Command]) extends Command
  final case class PredecessorUpdateResponse(i: Int, predecessor: ActorRef[Server.Command], predecessorHashValue: Int) extends Command
  final case object PredecessorUpdateResponseError extends Command

  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)

  def apply(m: Int, slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]],
            successor: ActorRef[Server.Command], predecessor: ActorRef[Server.Command]): Behavior[Command] = {

      def process(m: Int, slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]],
                  successor: ActorRef[Server.Command], predecessor: ActorRef[Server.Command]): Behavior[Command] =
        Behaviors.receive((context, message) => {
          val hashValue = ringValue(m, ByteBuffer.wrap(md5(context.self.toString)).getInt)

          context.log.info(s"${context.self.path} : my name is ${context.self.toString}")
          context.log.info(s"${context.self.path} : my hash is $hashValue")

          message match {
            case Parent.Join(successorNodeRef, predecessorNodeRef) =>
              if (successorNodeRef == context.self) {
                /*
                 * This is the first node to join the ring
                 */
                val newSlotToHash = mutable.Map[Int, Int]()
                val newHashToRef = mutable.Map[Int, ActorRef[Server.Command]]()
                (0 until m).foreach(i => {
                  newSlotToHash += (i -> hashValue)
                  newHashToRef += (hashValue -> context.self)
                })
                process(m, newSlotToHash, newHashToRef, successor, predecessorNodeRef)
              }

              else {
                /*
                 * There is at least 1 other node already in the ring
                 * Get the successor's finger table and use it to initialize new node's finger table
                 */
                implicit val timeout: Timeout = 5.seconds
                val newSlotToHash = mutable.Map[Int, Int]()
                val newHashToRef = mutable.Map[Int, ActorRef[Server.Command]]()
                var newPredecessor: ActorRef[Server.Command] = null

                context.ask(successorNodeRef, GetActorState) {
                  case x @ Success(ActorStateResponse(successorSlotToHash, successorHashToRef, successorHashValue, successorPredecessor)) =>
                    context.log.info(s"${context.self.path}\t:\tHash value = $hashValue\t:\tStarting to initialize finger table...")

                    newSlotToHash += (0 -> successorHashValue)
                    newHashToRef += (successorHashValue -> successorNodeRef)
                    newPredecessor = successorPredecessor

                    context.log.info(s"New predecessor found is $newPredecessor")
                    context.log.info(s"(i = 0) => (fingerNode = $successorNodeRef\t;\thash = $successorHashValue)")
                    successor ! FindSuccessor(1, ((hashValue + Math.pow(2, 1)) % Math.pow(2, m)).toInt, successor, context.self)

                    /*
                     * Fill in the new node's finger table using already existing successor's finger table
                     */
                    /*if ((hashValue + Math.pow(2, i)) <= newSlotToHash(0)) {
                        context.log.info(s"($hashValue + 2 ^ $i) % (2 ^ $m) = ${(hashValue + Math.pow(2, i)) % Math.pow(2, m)} <= ${newSlotToHash(0)}")
                        newSlotToHash += (i -> newSlotToHash(0))
                        newHashToRef += (newSlotToHash(i) -> newHashToRef(newSlotToHash(i)))
                      }

                      else {
                        // Here you cannot just take the previous finger table entry from the successor's finger table
                        // Instead tell your successor to find the node with hash (hashValue + 2 ^ i) % (2 ^ m) and enter that
                        // in your finger table's current index
                        /*newSlotToHash += (i -> successorSlotToHash(index))
                        index += 1
                        newHashToRef += (newSlotToHash(i) -> successorHashToRef(newSlotToHash(i)))*/

                      }*/

                    // context.log.info(s"(i = $i) => (actorNode = ${successorHashToRef(newSlotToHash(i))}\t;\thash = ${newSlotToHash(i)})")

//                    context.log.info(s"${context.self.path}\t:\thash = $hashValue\t:\tFinished initializing finger table.")
//                    context.log.info(s"Initialized finger tables: \nslotToHash - $newSlotToHash\nhashToRef - $newHashToRef")
                    x.value
                  case Failure(_) => ActorStateResponseError
                }

                process(m, newSlotToHash, newHashToRef, successorNodeRef, newPredecessor)
              }

            case GetFingerTable(replyTo) =>
              context.log.info(s"${context.self.path} : got request for finger table from $replyTo")
              replyTo ! FingerTableResponse(slotToHash, hashToRef)
              Behaviors.same

            case FingerTableResponse(slotToHash, hashToRef) =>
              context.log.info(s"${context.self.path} : Received this finger table - ${slotToHash.toString()} and ${hashToRef.toString()}")
              Behaviors.same

            case FingerTableResponseError =>
              context.log.error(s"${context.self.path} : could not receive finger table")
              Behaviors.same

            case GetHashValue(replyTo) =>
              context.log.info(s"${context.self.path} : got request for hash value from $replyTo")
              replyTo ! HashResponse(hashValue)
              Behaviors.same

            case HashResponse(hashValue) =>
              context.log.info(s"${context.self.path} : received this hash value - $hashValue")
              Behaviors.same

            case HashResponseError =>
              context.log.error(s"${context.self.path} : could not receive hash value")
              Behaviors.same

            case GetPredecessor(replyTo) =>
              context.log.info(s"${context.self.path} : got request for predecessor from $replyTo")
              replyTo ! PredecessorResponse(predecessor)
              Behaviors.same

            case PredecessorResponse(predecessor) =>
              context.log.info(s"${context.self.path} : got this predecessor value - $predecessor")
              Behaviors.same

            case GetActorState(replyTo) =>
              context.log.info(s"${context.self.path} : got request for actor state - $replyTo")
              replyTo ! ActorStateResponse(slotToHash, hashToRef, hashValue, predecessor)
              Behaviors.same

            case ActorStateResponse(_, _, _, _) =>
              context.log.info(s"${context.self.path} : got actor state response")
              Behaviors.same

            case ActorStateResponseError =>
              context.log.error(s"${context.self.path} : could not get actor state response")
              Behaviors.same

            case FindSuccessorToFindData(id, srcRouteRef) =>
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x @ Success(HashResponse(successorHashValue)) =>
                  findSuccessorToFindData(id, srcRouteRef, hashValue, successorHashValue, context)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case FindPredecessorToFindData(id, replyTo, srcRouteRef) =>
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x @ Success(HashResponse(successorHashValue)) =>
                  findPredecessorToFindData(id, replyTo, srcRouteRef, hashValue, successorHashValue, context)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case SuccessorFoundForData(id, successorRef, srcRouteRef) =>
              successorRef ! GetData(id, srcRouteRef)
              Behaviors.same

            case SuccessorNotFound(id, srcRouteRef) =>
              srcRouteRef ! DataResponseFailed(s"Could not find data corresponding to hash value $id")
              Behaviors.same

            case GetData(_, srcRouteRef) =>
              srcRouteRef ! DataResponseSuccess(Some(Data("", 0, "")))
              Behaviors.same

            case FindSuccessor(i, id, replyTo, newNodeRef) =>
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x @ Success(HashResponse(successorHashValue)) =>
                  findSuccessor(i, id, replyTo, newNodeRef, hashValue, successorHashValue, context)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case FindPredecessor(i, id, replyTo, newNodeRef) =>
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x @ Success(HashResponse(successorHashValue)) =>
                  findPredecessor(i, id, replyTo, newNodeRef, hashValue, successorHashValue, context)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case SuccessorFound(i, id, successor, successorHashValue, newNodeRef) =>
              context.log.info(s"${context.self.path} : node $successor found as finger ${i+1} for $newNodeRef")
              newNodeRef ! NewSuccessorResponse(i, successor, successorHashValue)
              Behaviors.same

            case NewSuccessorResponse(i, newSuccessor, newSuccessorHashValue) =>
              context.log.info(s"${context.self.path}\t:\t(i = $i) => fingerNode = $newSuccessor\thash = $newSuccessorHashValue")
              if (i <= m) {
                successor ! FindSuccessor(i+1, ((hashValue+Math.pow(2, i+1)) % Math.pow(2, m)).toInt, successor, context.self)
                process(m, slotToHash+(i->newSuccessorHashValue), hashToRef+(i->newSuccessor), successor, predecessor)
              }
              else {
                findPredecessorToUpdate(1, hashValue-Math.pow(2, 1).toInt, context.self, hashValue, slotToHash(0), context)
                Behaviors.same
              }

            case NewSuccessorResponseError => Behaviors.same

            case UpdateFingerTable(newNode, newNodeHashValue, i) =>
              updateFingerTable(newNode, newNodeHashValue, i, hashValue, slotToHash(0))
              Behaviors.same

            case FindPredecessorToUpdate(i, id, replyTo) =>
              findPredecessorToUpdate(i, id, replyTo, hashValue, slotToHash(0), context)
              Behaviors.same

            case PredecessorUpdateResponse(i, predecessor, predecessorHashValue) =>
              predecessor ! UpdateFingerTable(context.self, hashValue, i)
              predecessor ! FindPredecessorToUpdate(i+1, hashValue-Math.pow(2, i+1).toInt, context.self)
              Behaviors.same
          }
        })

    def findSuccessorToFindData(id: Int, srcRouteRef: ActorRef[DataActionResponse], hashValue: Int, successorHashValue: Int,
                      context: ActorContext[Command]): Behavior[Command] =
      findPredecessorToFindData(id, context.self, srcRouteRef, hashValue, successorHashValue, context)

    def findPredecessorToFindData(id: Int, replyTo: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse],
                       hashValue: Int, successorHashValue: Int, context: ActorContext[Command]): Behavior[Command] = {
      if (!isInLeftOpenInterval(id, hashValue, successorHashValue) && successor == replyTo)
        replyTo ! SuccessorNotFound(id, srcRouteRef)
      else if (!isInLeftOpenInterval(id, hashValue, successorHashValue)) {
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context)
        closestPrecedingNodeRef ! FindPredecessorToFindData(id, replyTo, srcRouteRef)
      }
      else replyTo ! SuccessorFoundForData(id, successor, srcRouteRef)
      Behaviors.same
    }

    def findClosestPrecedingFinger(id: Int, hashValue: Int, context: ActorContext[Command]): ActorRef[Command] = {
      ((m-1) to 0 by -1).foreach(i =>
        if ((slotToHash(i) % Math.pow(2, m)) > hashValue && (slotToHash(i) % Math.pow(2, m)) < id) return hashToRef(slotToHash(i)))
      context.self
    }


    def findSuccessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command], hashValue: Int, successorHashValue: Int,
                      context: ActorContext[Command]): Behavior[Command] =
      findPredecessor(i: Int, id, replyTo, newNodeRef, hashValue, successorHashValue, context)

    def findPredecessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command], hashValue: Int, successorHashValue: Int,
                        context: ActorContext[Command]): Behavior[Command] = {
      context.log.info(s"${context.self.path}\t:\tid = $id\thash = $hashValue\tsuccessorHash=$successorHashValue")
      if (!isInLeftOpenInterval(id, hashValue, successorHashValue) && successor == replyTo) {
        context.log.info(s"${context.self.path}\t:\t$successorHashValue is the successor node key for key $id")
        replyTo ! SuccessorFound(i, id, successor, successorHashValue, newNodeRef)
      }
      else if (!isInLeftOpenInterval(id, hashValue, successorHashValue)) {
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context)
        context.log.info(s"${context.self.path}\t:\t sending search for $id to node $closestPrecedingNodeRef")
        closestPrecedingNodeRef ! FindPredecessor(i, id, replyTo, newNodeRef)
      }

      else {
        context.log.info(s"${context.self.path}\t:\tfound successor node with key $successorHashValue for key $id")
        replyTo ! SuccessorFound(i, id, successor, successorHashValue, newNodeRef)
      }
      Behaviors.same
    }

    def findPredecessorToUpdate(i: Int, id: Int, replyTo: ActorRef[Command], hashValue: Int, successorHashValue: Int,
                               context: ActorContext[Command]): Behavior[Command] = {
      if (isInLeftOpenInterval(id, hashValue, successorHashValue))
        replyTo ! PredecessorUpdateResponse(i, successor, successorHashValue)
      else {
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context)
        closestPrecedingNodeRef ! FindPredecessorToUpdate(i, id, replyTo)
      }
      Behaviors.same
    }

    def updateFingerTable(newNode: ActorRef[Command], hashValue: Int, i: Int, predecessorHashValue: Int,
                          predecessorSuccessorHashValue: Int): Unit = {
      if (isInRightOpenInterval(hashValue, predecessorHashValue, predecessorSuccessorHashValue)) {
        hashToRef += (slotToHash(i) -> newNode)
        predecessor ! UpdateFingerTable(newNode, hashValue, i)
      }
    }

    def ringValue(m: Int, n: Int): Int = (n % Math.pow(2, m)).toInt

    def isInLeftOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean =
      if (successorHash >= hash) id > hash && id <= successorHash
      else (id >= hash && id < 0) || (id > hash && (id >= 0 && id < successorHash))

    def isInRightOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean =
      if (successorHash >= hash) id >= hash && id < successorHash
      else (id >= hash && id < 0) || (id >= 0 && id < successorHash)

    process(m, slotToHash, hashToRef, successor, predecessor)
  }
}
