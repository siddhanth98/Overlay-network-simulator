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
   */
  final case class FindSuccessor(id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class FindPredecessor(id: Int, replyTo: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class SuccessorFound(id: Int, successorRef: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class SuccessorNotFound(id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command


  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)

  def apply(m: Int, slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]], successor: ActorRef[Server.Command],
            predecessor: ActorRef[Server.Command]): Behavior[Command] = {

      def process(m: Int, slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]], successor: ActorRef[Server.Command],
                 predecessor: ActorRef[Server.Command]): Behavior[Command] =
        Behaviors.receive((context, message) => {
          val hashValue = ByteBuffer.wrap(md5(context.self.toString)).getInt

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
                implicit val timeout: Timeout = 3.seconds
                val newSlotToHash = mutable.Map[Int, Int]()
                val newHashToRef = mutable.Map[Int, ActorRef[Server.Command]]()
                var newPredecessor: ActorRef[Server.Command] = null

                context.ask(successorNodeRef, GetActorState) {
                  case x @ Success(ActorStateResponse(successorSlotToHash, successorHashToRef, successorHashValue, successorPredecessor)) =>
                    context.log.info(s"${context.self.path} : Hash value = $hashValue : Starting to initialize finger table...")

                    newSlotToHash += (0 -> successorHashValue)
                    newHashToRef += (successorHashValue -> successorNodeRef)
                    newPredecessor = successorPredecessor

                    context.log.info(s"New predecessor found is $newPredecessor")
                    context.log.info(s"(i = 0) => (actorNode = $successorNodeRef\t;\thash = $successorHashValue)")

                    /*
                     * Fill in the new node's finger table using already existing successor's finger table
                     */
                    var index = 0
                    (1 until m).foreach(i => {
                      if (((hashValue + Math.pow(2, i)) % Math.pow(2, m)) <= newSlotToHash(0)) {
                        context.log.info(s"($hashValue + 2 ^ $i) % (2 ^ $m) = ${(hashValue + Math.pow(2, i)) % Math.pow(2, m)} <= ${newSlotToHash(0)}")
                        newSlotToHash += (i -> newSlotToHash(0))
                        newHashToRef += (newSlotToHash(i) -> newHashToRef(newSlotToHash(i)))
                      }

                      else {
                        newSlotToHash += (i -> successorSlotToHash(index))
                        index += 1
                        newHashToRef += (newSlotToHash(i) -> successorHashToRef(newSlotToHash(i)))
                      }
                      context.log.info(s"(i = $i) => (actorNode = ${successorHashToRef(newSlotToHash(i))}\t;\thash = ${newSlotToHash(i)})")
                    })
                    context.log.info(s"${context.self.path}\t:\thash = $hashValue\t:\tFinished initializing finger table.")
                    context.log.info(s"Initialized finger tables: \nslotToHash - $newSlotToHash\nhashToRef - $newHashToRef")
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

            case FindSuccessor(id, srcRouteRef) =>
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x @ Success(HashResponse(successorHashValue)) =>
                  findSuccessor(id, srcRouteRef, hashValue, successorHashValue, context)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case FindPredecessor(id, replyTo, srcRouteRef) =>
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x @ Success(HashResponse(successorHashValue)) =>
                  findPredecessorToFindData(id, replyTo, srcRouteRef, hashValue, successorHashValue, context)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case SuccessorFound(id, successorRef, srcRouteRef) =>
              successorRef ! GetData(id, srcRouteRef)
              Behaviors.same

            case SuccessorNotFound(id, srcRouteRef) =>
              srcRouteRef ! DataResponseFailed(s"Could not find data corresponding to hash value $id")
              Behaviors.same

            case GetData(_, srcRouteRef) =>
              srcRouteRef ! DataResponseSuccess(Some(Data("", 0, "")))
              Behaviors.same
          }
        })

    def findSuccessor(id: Int, srcRouteRef: ActorRef[DataActionResponse], hashValue: Int, successorHashValue: Int,
                      context: ActorContext[Command]): Behavior[Command] =
      findPredecessorToFindData(id, context.self, srcRouteRef, hashValue, successorHashValue, context)

    def findPredecessorToFindData(id: Int, replyTo: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse],
                       hashValue: Int, successorHashValue: Int, context: ActorContext[Command]): Behavior[Command] = {
      if (!(id > hashValue && id <= successorHashValue) && successor == replyTo)
        replyTo ! SuccessorNotFound(id, srcRouteRef)
      else if (!(id > hashValue && id <= successorHashValue)) {
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context)
        closestPrecedingNodeRef ! FindPredecessor(id, replyTo, srcRouteRef)
      }
      else replyTo ! SuccessorFound(id, successor, srcRouteRef)
      Behaviors.same
    }

    def findClosestPrecedingFinger(id: Int, hashValue: Int, context: ActorContext[Command]): ActorRef[Command] = {
      ((m-1) to 0 by -1).foreach(i => if ((slotToHash(i) % Math.pow(2, m)) > hashValue && (slotToHash(i) % Math.pow(2, m)) < id) return hashToRef(slotToHash(i)))
      context.self
    }
      process(m, slotToHash, hashToRef, successor, predecessor)
  }
}
