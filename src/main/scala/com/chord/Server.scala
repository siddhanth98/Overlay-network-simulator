package com.chord

import java.nio.ByteBuffer

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import java.security.MessageDigest

import scala.collection.mutable
import scala.math.BigInt.javaBigInteger2bigInt

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
  final case class SetPredecessor(predecessor: ActorRef[Command]) extends Command

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
  final case class FindSuccessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command],
                                 newNodeHashValue: Int) extends Command
  final case class FindPredecessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command],
                                  newNodeHashValue: Int) extends Command
  final case class SuccessorFound(i: Int, id: Int, successor: ActorRef[Command], successorHashValue: Int,
                                  newNodeRef: ActorRef[Command]) extends Command
  final case class NewSuccessorResponse(i: Int, successor: ActorRef[Command], successorHashValue: Int) extends Command
  final case object NewSuccessorResponseError extends Command
  final case class UpdateFingerTableAndSuccessor(newSuccessor: ActorRef[Command], i: Int, newHash: Int,
                                                 newHashRef: ActorRef[Command]) extends Command

  /**
   * Set of messages to update finger tables of new node's predecessors
   */
  final case class UpdateFingerTable(newNode: ActorRef[Command], hashValue: Int, i: Int) extends Command
  final case class FindPredecessorToUpdate(i: Int, id: Int, replyTo: ActorRef[Server.Command]) extends Command
  final case class PredecessorUpdateResponse(i: Int, predecessor: ActorRef[Server.Command], predecessorHashValue: Int) extends Command
  final case object PredecessorUpdateResponseError extends Command

  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)
  def getSignedHash(m: Int, s: String): Int = (UnsignedInt(ByteBuffer.wrap(md5(s)).getInt).bigIntegerValue % Math.pow(2, m).toInt).intValue()
  def getSignedHashOfRingSlotNumber(m: Int, slotNo: Int): Int = (UnsignedInt(slotNo).bigIntegerValue % Math.pow(2, m).toInt).intValue()

  def apply(m: Int, slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]],
            successor: ActorRef[Server.Command], predecessor: ActorRef[Server.Command]): Behavior[Command] = {

      def process(m: Int, slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Server.Command]],
                  successor: ActorRef[Server.Command], predecessor: ActorRef[Server.Command]): Behavior[Command] = {
        Behaviors.receive((context, message) => {
//          val hashValue = ringValue(m, ByteBuffer.wrap(md5(context.self.path.toString)).getInt) % Math.pow(2, m).round.toInt
          val hashValue = getSignedHash(m, context.self.path.toString)

          context.log.info(s"${context.self.path}\t:\trestarted behavior\t-\thashValue=$hashValue\tsuccessor=$successor\tpredecessor=$predecessor")

          context.log.info(s"${context.self.path}\t:\tHash value = $hashValue")

          message match {
            case Parent.Join(successorNodeRef, predecessorNodeRef) =>
              if (successorNodeRef == context.self) {
                /*
                 * This is the first node to join the ring
                 */
                context.log.info(s"${context.self.path}\t:\tI am the first node to join the ring")
                val newSlotToHash = mutable.Map[Int, Int]()
                val newHashToRef = mutable.Map[Int, ActorRef[Server.Command]]()
                (0 until m).foreach(i => {
                  newSlotToHash += (i -> hashValue)
                  newHashToRef += (hashValue -> context.self)
                })
                context.log.info(s"${context.self.path}\t:\tInitialized finger tables\t:\tslotToHash = $newSlotToHash\thashToRef = $newHashToRef")
                process(m, newSlotToHash, newHashToRef, successorNodeRef, predecessorNodeRef)
              }

              else {
                /*
                 * There is at least 1 other node already in the ring
                 * Get the successor's finger table and use it to initialize new node's finger table
                 */
                context.log.info(s"${context.self.path}\t:\tI am not the first node to join the ring")
                context.log.info(s"${context.self.path}\t:\tAsking successor node $successorNodeRef for its state...")
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

                    context.log.info(s"${context.self.path}\t:\tNew predecessor found is $newPredecessor")
                    context.log.info(s"${context.self.path}\t:\tletting successor node $successorNodeRef know that I am its new predecessor")
                    successorNodeRef ! SetPredecessor(context.self)

                    context.log.info(s"${context.self.path}\t:\t(i = 0) => (fingerNode = $successorNodeRef\t;\thash = $successorHashValue)")
                    successorNodeRef ! FindSuccessor(1, ((hashValue + Math.pow(2, 1).round) % Math.pow(2, m)).toInt, successorNodeRef, context.self, hashValue)

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

                process(m, newSlotToHash, newHashToRef, successorNodeRef, predecessorNodeRef)
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

            case SetPredecessor(newPredecessor) =>
              context.log.info(s"${context.self.path}\t:\tgot $newPredecessor as my new predecessor. Resetting $predecessor to $newPredecessor")
              process(m, slotToHash, hashToRef, successor, newPredecessor)

            case UpdateFingerTableAndSuccessor(newSuccessor, i, newHash, newHashRef) =>
              context.log.info(s"${context.self.path}\t:\tgot new finger tables:\tslotToHash = ${slotToHash+(i -> newHash)}\thashToRef = ${hashToRef+(newHash -> newHashRef)}")
              newSuccessor match {
                case null => process(m, slotToHash+(i -> newHash), hashToRef+(newHash -> newHashRef), successor, predecessor)
                case _ =>
                  context.log.info(s"${context.self.path}\t:\tgot $newSuccessor as my new successor. Resetting successor $successor to $newSuccessor")
                  process(m, slotToHash+(i -> newHash), hashToRef+(newHash -> newHashRef), newSuccessor, predecessor)
              }

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
              findSuccessorToFindData(id, srcRouteRef, hashValue, slotToHash(0), context, slotToHash, hashToRef)
              Behaviors.same

            case FindPredecessorToFindData(id, replyTo, srcRouteRef) =>
              findPredecessorToFindData(id, replyTo, srcRouteRef, hashValue, slotToHash(0), context, slotToHash, hashToRef)
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

            case FindSuccessor(i, id, replyTo, newNodeRef, newNodeHashValue) =>
              context.log.info(s"${context.self.path}\t:\tGot request to find finger successor of node with hash $id for $newNodeRef")
              context.log.info(s"${context.self.path}\t:\tasking my successor node $successor for its hash value")
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x@Success(HashResponse(successorHashValue)) =>
                  findSuccessor(i, id, replyTo, newNodeRef, newNodeHashValue, hashValue, successorHashValue, context, slotToHash, hashToRef)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case FindPredecessor(i, id, replyTo, newNodeRef, newNodeHashValue) =>
              context.log.info(s"${context.self.path}\t:\tgot request to find successor of key $id for $newNodeRef")
              context.log.info(s"${context.self.path}\t:\tasking my successor node $successor for its hash value")
              implicit val timeout: Timeout = 5.seconds
              context.ask(successor, GetHashValue) {
                case x@Success(HashResponse(successorHashValue)) =>
                  findPredecessor(i, id, replyTo, newNodeRef, newNodeHashValue, hashValue, successorHashValue, context, slotToHash, hashToRef)
                  x.value
                case Failure(_) => HashResponseError
              }
              Behaviors.same

            case SuccessorFound(i, id, successor, successorHashValue, newNodeRef) =>
              context.log.info(s"${context.self.path} : node $successor with hash $successorHashValue found as " +
                s"successor of key $id for $newNodeRef")
              newNodeRef ! NewSuccessorResponse(i, successor, successorHashValue)
              Behaviors.same

            case NewSuccessorResponse(i, newSuccessor, newSuccessorHashValue) =>
              if (i < (m)) {
                context.log.info(s"${context.self.path}\t:\t(i = $i) => (fingerNode = $newSuccessor\t;\thash = $newSuccessorHashValue)")
                successor ! FindSuccessor(i+1, ((hashValue+Math.pow(2, i+1).round) % Math.pow(2, m)).toInt, successor, context.self, hashValue)
                process(m, slotToHash+(i->newSuccessorHashValue), hashToRef+(newSuccessorHashValue->newSuccessor), successor, predecessor)
              }
              else {
                context.log.info(s"${context.self.path}\t:\tInitialized finger tables: \nslotToHash - $slotToHash\nhashToRef - $hashToRef")
                context.log.info(s"${context.self.path}\t:\tFinding my predecessor 0 to update")
                findPredecessorToUpdate(0, getSignedHashOfRingSlotNumber(m, (hashValue-Math.pow(2, 0).round).toInt),
                  context.self, hashValue, slotToHash(0), context, slotToHash, hashToRef)
                Behaviors.same
              }

            case NewSuccessorResponseError => Behaviors.same

            case UpdateFingerTable(newNode, newNodeHashValue, i) =>
              context.log.info(s"${context.self.path}\t:\tGot request to update finger $i in my finger table if required")
              updateFingerTable(newNode, newNodeHashValue, i, hashValue, slotToHash(i), context, slotToHash, hashToRef, predecessor)
              Behaviors.same

            case FindPredecessorToUpdate(i, id, replyTo) =>
              context.log.info(s"${context.self.path}\t:\tGot request to find successor of key $id")
              findPredecessorToUpdate(i, id, replyTo, hashValue, slotToHash(0), context, slotToHash, hashToRef)
              Behaviors.same

            case PredecessorUpdateResponse(i, predecessor, predecessorHashValue) =>
              context.log.info(s"${context.self.path}\t:\tGot predecessor $i as node $predecessor with hash $predecessorHashValue")

              if (predecessor != context.self) {
                context.log.info(s"${context.self.path}\t:\tSending update finger table finger $i message to predecessor $predecessor")
                predecessor ! UpdateFingerTable(context.self, hashValue, i)
              }

              if (i < (m-1)) {
                context.log.info(s"${context.self.path}\t:\tFinding predecessor ${i + 1} to update")
                findPredecessorToUpdate(i + 1, getSignedHashOfRingSlotNumber(m, (hashValue - Math.pow(2, i + 1).round).toInt),
                  context.self, hashValue, slotToHash(0), context, slotToHash, hashToRef)
              }
              else {
                context.log.info(s"${context.self.path}\t:\tFinished telling all my predecessors to update their finger tables")
              }
              Behaviors.same
          }
        })
      }

    def findSuccessorToFindData(id: Int, srcRouteRef: ActorRef[DataActionResponse], hashValue: Int, successorHashValue: Int,
                      context: ActorContext[Command], slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] =
      findPredecessorToFindData(id, context.self, srcRouteRef, hashValue, successorHashValue, context, slotToHash, hashToRef)

    def findPredecessorToFindData(id: Int, replyTo: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse],
                       hashValue: Int, successorHashValue: Int, context: ActorContext[Command],
                                  slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] = {
      context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tGot request for processing data key $id")
      if (id == hashValue) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tI have the data for key $id. Returning myself.")
        replyTo ! SuccessorFoundForData(id, context.self, srcRouteRef)
      }
      else if (hashValue != successorHashValue && isInLeftOpenInterval(id, hashValue, successorHashValue)) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tSuccessor with key $successorHashValue has the key $id. " +
          s"Returning ${hashToRef(slotToHash(0))}")
        replyTo ! SuccessorFoundForData(id, hashToRef(slotToHash(0)), srcRouteRef)
      }
      else if (hashValue != successorHashValue) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tFinding the closest preceding node to key $id")
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context, slotToHash, hashToRef)
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tClosest preceding node found is $closestPrecedingNodeRef")
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tSending search to node $closestPrecedingNodeRef")
        replyTo ! FindSuccessorToFindData(id, srcRouteRef)
      }
      else {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tCould not find node for data key $id")
        replyTo ! SuccessorNotFound(id, srcRouteRef)
      }
      Behaviors.same
    }

    def findClosestPrecedingFinger(id: Int, hashValue: Int, context: ActorContext[Command], slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]]): ActorRef[Command] = {
      ((m-1) to 0 by -1).foreach(i => {
        if (slotToHash.contains(i) && isInOpenInterval(slotToHash(i), hashValue, id)) return hashToRef(slotToHash(i))
      })
      context.self
    }

    def findSuccessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command], newNodeHashValue: Int, hashValue: Int, successorHashValue: Int,
                      context: ActorContext[Command], slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] =
      findPredecessor(i: Int, id, replyTo, newNodeRef, newNodeHashValue, hashValue, successorHashValue, context, slotToHash, hashToRef)

    def findPredecessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command], newNodeHashValue: Int, hashValue: Int, successorHashValue: Int,
                        context: ActorContext[Command], slotToHash: mutable.Map[Int, Int],
                        hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] = {
      context.log.info(s"${context.self.path}\t:\tid = $id\thash = $hashValue\tsuccessorHash=$successorHashValue")
      /*
       * Check if requested key successor is between myself and new node's position (clockwise direction), if it is then new node itself is the successor
       * otherwise if it is between new node and myself then I am the successor
       */
      if (hashValue == successorHashValue && isInLeftOpenInterval(id, hashValue, newNodeHashValue)) {
        // Successor node of "id" found between myself and new node
        context.log.info(s"${context.self.path}\t:\t$newNodeHashValue is the successor node key for key $id")
        replyTo ! SuccessorFound(i, id, newNodeRef, newNodeHashValue, newNodeRef)
      }

      else if (isInLeftOpenInterval(id, hashValue, successorHashValue)) {
        // Successor node of "id" found between myself and my successor
        context.log.info(s"${context.self.path}\t:\t$successorHashValue is the successor node key for key $id")
        replyTo ! SuccessorFound(i, id, hashToRef(slotToHash(0)), successorHashValue, newNodeRef)
      }

      else {
        context.log.info(s"${context.self.path}\t:\tFinding closest preceding node of key $id...")
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context, slotToHash, hashToRef)
        if (closestPrecedingNodeRef == context.self) {
          /*
           * Closest preceding node found is myself, so I will check if the successor is between the new node and myself,
           * or between myself and the new node
           */
          implicit val timeout: Timeout = 3.seconds
          context.ask(newNodeRef, GetHashValue) {
            case x @ Success(HashResponse(newNodeHashValue)) =>
              if (isInLeftOpenInterval(id, hashValue, newNodeHashValue)) {
                context.log.info(s"${context.self.path}\t:\tkey $id found between keys $hashValue and $newNodeHashValue")
                replyTo ! SuccessorFound(i, id, newNodeRef, newNodeHashValue, newNodeRef)
              } else if (isInLeftOpenInterval(id, newNodeHashValue, hashValue)) {
                context.log.info(s"${context.self.path}\t:\tkey $id found between keys $newNodeHashValue and $hashValue")
                replyTo ! SuccessorFound(i, id, context.self, hashValue, newNodeRef)
              }
              x.value
            case Failure(_) => HashResponseError
          }
        }
        else {
          context.log.info(s"${context.self.path}\t:\t sending search for key $id to node $closestPrecedingNodeRef")
          closestPrecedingNodeRef ! FindPredecessor(i, id, replyTo, newNodeRef, newNodeHashValue)
        }
      }

      Behaviors.same
    }

    def findPredecessorToUpdate(i: Int, id: Int, replyTo: ActorRef[Command], hashValue: Int, successorHashValue: Int,
                               context: ActorContext[Command], slotToHash: mutable.Map[Int, Int],
                                hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] = {
      context.log.info(s"${context.self.path}\t:\tfinding successor of key $id")
      if (isInLeftOpenInterval(id, hashValue, successorHashValue)) {
        context.log.info(s"${context.self.path}\t:\tsuccessor of key $id found in left open interval ($hashValue, $successorHashValue]")
        if (id != successorHashValue)
          replyTo ! PredecessorUpdateResponse(i, context.self, hashValue)
        else replyTo ! PredecessorUpdateResponse(i, hashToRef(slotToHash(0)), slotToHash(0))
      } else {
        context.log.info(s"${context.self.path}\t:\tFinding closest preceding node of key $id...")
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context, slotToHash, hashToRef)
        context.log.info(s"${context.self.path}\t:\tClosest preceding node found is $closestPrecedingNodeRef")

        if (closestPrecedingNodeRef == context.self) {
          /*
           * Closest preceding node found is myself, so I will check if the successor is between the new node and myself,
           * or between myself and the new node
           */
          /*implicit val timeout: Timeout = 5.seconds
          context.ask(replyTo, GetHashValue) {
            case x @ Success(HashResponse(newNodeHashValue)) =>
              if (isInLeftOpenInterval(id, hashValue, newNodeHashValue)) {
                context.log.info(s"${context.self.path}\t:\tkey $id found between keys $hashValue and $newNodeHashValue")
                replyTo ! PredecessorUpdateResponse(i, replyTo, newNodeHashValue)
              } else if (isInLeftOpenInterval(id, newNodeHashValue, hashValue)) {
                context.log.info(s"${context.self.path}\t:\tkey $id found between keys $newNodeHashValue and $hashValue")
                replyTo ! PredecessorUpdateResponse(i, context.self, hashValue)
              }
              x.value
            case Failure(_) => HashResponseError
          }*/
        }
        else {
          context.log.info(s"${context.self.path}\t:\tcould not find successor of key $id. Sending search to $closestPrecedingNodeRef")
          closestPrecedingNodeRef ! FindPredecessorToUpdate(i, id, replyTo)
        }
      }
      Behaviors.same
    }

    def updateFingerTable(newNode: ActorRef[Command], newNodeHashValue: Int, i: Int, predecessorHashValue: Int,
                          predecessorSuccessorHashValue: Int, context: ActorContext[Command],
                          slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]],
                         predecessor: ActorRef[Command]): Unit = {
      context.log.info(s"${context.self.path}\t:\tchecking whether I can update my finger $i to node with hash $newNodeHashValue")
      context.log.info(s"${context.self.path}\t:\tchecking whether node with hash $newNodeHashValue succeeds my current " +
        s"successor node with hash ${slotToHash(i)}")
      if ((newNode != context.self && isInRightOpenInterval(newNodeHashValue, predecessorHashValue, slotToHash(i))) ||
        (i == 0 && predecessorHashValue == predecessorSuccessorHashValue)) {
        context.log.info(s"${context.self.path}\t:\tUpdating finger $i in my finger tables: old data => " +
          s"slotToHash = $slotToHash\t;\thashToRef = $hashToRef")
        if (i == 0) context.self ! UpdateFingerTableAndSuccessor(newNode, i, newNodeHashValue, newNode)
        else context.self ! UpdateFingerTableAndSuccessor(null, i, newNodeHashValue, newNode)
        predecessor ! UpdateFingerTable(newNode, newNodeHashValue, i)
      }
    }

    def ringValue(m: Int, n: Int): Int = (n % Math.pow(2, m)).toInt

    def isInLeftOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean =
      if (successorHash == hash || id == successorHash) true
      else if (successorHash > hash) id > hash && id <= successorHash
      else (id > hash) || (id >= 0 && id < successorHash)

    def isInRightOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean = {
      if (successorHash == hash) true
      else if (successorHash > hash) id >= hash && id < successorHash
      else (id >= hash) || (id >= 0 && id < successorHash)
    }

    def isInOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean = {
      if (successorHash == hash) true
      else if (successorHash > hash) id > hash && id < successorHash
      else (id > hash) || (id >= 0 && id < successorHash)
    }

    process(m, slotToHash, hashToRef, successor, predecessor)
  }
}
