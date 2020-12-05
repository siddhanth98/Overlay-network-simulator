package com.chord

import java.nio.ByteBuffer
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout
import com.utils.UnsignedInt

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import java.security.MessageDigest
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.math.BigInt.javaBigInteger2bigInt

/**
 * The server actor represents the chord node in the overlay network
 * An actor's finger table is represented using the following 2 maps:
 *  slotToHash - Mapping from i(0...m-1) to corresponding finger's hash value
 *  hashToRef - Mapping from a finger's hash value to the corresponding actor's reference
 * Though these are defined as mutable maps, they are not mutated inside any function, but
 * just passed around as arguments
 */
object Node {

  trait Command
  sealed trait DataActionResponse extends Command with Parent.Command
  final val logger: Logger = LoggerFactory.getLogger(Node.getClass)

  /**
   * Set of messages for querying and obtaining finger tables
   */
  final case class GetFingerTable(replyTo: ActorRef[FingerTableResponse]) extends Command
  final case class FingerTableResponse(slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Node.Command]]) extends Command
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
  final case class PredecessorResponse(predecessor: ActorRef[Node.Command]) extends Command
  final case object PredecessorResponseError extends Command
  final case class SetPredecessor(predecessor: ActorRef[Command]) extends Command

  /**
   * Set of messages for querying and obtaining all relevant state properties from another server node.
   * State will have the following - Finger table maps, hash value and predecessor actor reference
   */
  final case class GetActorState(replyTo: ActorRef[ActorStateResponse]) extends Command
  final case class ActorStateResponse(slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Node.Command]],
                                      hashValue: Int, predecessor: ActorRef[Node.Command]) extends Command
  final case object ActorStateResponseError extends Command

  /**
   * Set of messages for representing data and data responses.
   * Data here represents a movie type having a name, size in MB and genre
   */
  final case class Data(name: String, size: Int, genre: String)
  final case class AllData(movies: Set[Data]) extends Command
  final case class GetData(name: String, id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class DataResponseSuccess(data: Option[Data]) extends DataActionResponse
  final case class DataResponseFailed(description: String) extends DataActionResponse
  final case class DataStorageResponseSuccess(description: String) extends DataActionResponse
  final case class DataStorageResponseFailed(description: String) extends DataActionResponse
  final case class StoreData(data: Data, id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class GetAllData(replyTo: ActorRef[AllData]) extends Command with Parent.Command

  /**
   * Set of messages for finding successors, predecessors and success and failure responses
   * when obtaining or storing data
   */
  final case class FindSuccessorToFindData(name: String, srcRouteRef: ActorRef[DataActionResponse]) extends Command with Parent.Command
  final case class FindPredecessorToFindData(name: String, id: Int, replyTo: ActorRef[Command],
                                             srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class SuccessorFoundForData(name: String, id: Int, successorRef: ActorRef[Command],
                                         srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class SuccessorNotFound(name: String, id: Int, srcRouteRef: ActorRef[DataActionResponse]) extends Command
  final case class FindNodeForStoringData(data: Data, srcRouteRef: ActorRef[DataActionResponse]) extends Command with Parent.Command

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
  final case class FindPredecessorToUpdate(i: Int, id: Int, replyTo: ActorRef[Node.Command]) extends Command
  final case class PredecessorUpdateResponse(i: Int, predecessor: ActorRef[Node.Command], predecessorHashValue: Int) extends Command
  final case object PredecessorUpdateResponseError extends Command
  final case class AllPredecessorsUpdated(replyTo: ActorRef[AllPredecessorsUpdatedResponse]) extends Command
  final case class AllPredecessorsUpdatedResponse(response: Boolean) extends Command

  /**
   * Object and message which define the node's state to be dumped in a yaml file
   */
  final case class StateToDump(nodeHash: Map[String, Int], fingerTable: List[mutable.Map[Int, Int]], movies: Set[Data])
  final case class ActorState(replyTo: ActorRef[Command], state: StateToDump) extends Command with Parent.Command

  /**
   * Md5 hash function to generate the hash value of the actor's fully qualified path name
   * @param s The actor's path name
   */
  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)

  /**
   * This function converts the md5 hash byte array computed above to an unsigned integer value to be used as the hash
   * value of an actor/data
   * @param m The number of finger table entries for each actor
   * @param s The actor's path name
   */
  def getSignedHash(m: Int, s: String): Int = (UnsignedInt(ByteBuffer.wrap(md5(s)).getInt).bigIntegerValue % Math.pow(2, m).toInt).intValue()

  /**
   * This function is used to wrap the signed hash value of an actor node when the following occurs:
   *    To find the predecessor node whose finger table should be updated, the new node will subtract Math.pow(2, i) from
   *    it's hash value, which can yield a negative number. This function will ensure that the result of the subtraction
   *    like -1 is properly considered as Math.pow(2, m)-1, which the node with the largest value
   * @param m The number of finger table entries
   * @param slotNo The slot number of the target predecessor resulting from the difference (hash_value - Math.pow(2, i))
   */
  def getSignedHashOfRingSlotNumber(m: Int, slotNo: Int): Int = (UnsignedInt(slotNo).bigIntegerValue % Math.pow(2, m).toInt).intValue()

  def apply(m: Int, slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Node.Command]],
            successor: ActorRef[Node.Command], predecessor: ActorRef[Node.Command]): Behavior[Command] = {

    /**
     * This method defines the behavior of an actor node, subject to receipt of the above specified messages
     * @param predecessorsUpdated A flag which indicates whether or not a new node in the ring has notified every one of its
     *                            predecessors to update their finger tables.
     * @param movies Set of movies that this actor node currently stores
     * @param m Number of finger table entries that the actor node will have
     * @param slotToHash The map from i to finger's hash value
     * @param hashToRef The map from a finger's hash value to the actor's reference
     * @param successor The successor actor's reference
     * @param predecessor The predecessor actor's reference
     */
      def process(predecessorsUpdated: Boolean, movies: Set[Data], m: Int, slotToHash: mutable.Map[Int, Int],
                  hashToRef: mutable.Map[Int, ActorRef[Node.Command]], successor: ActorRef[Node.Command],
                  predecessor: ActorRef[Node.Command]): Behavior[Command] = {
        Behaviors.receive((context, message) => {
          val hashValue = getSignedHash(m, context.self.path.toString)

          message match {
            case Parent.Join(successorNodeRef, predecessorNodeRef) =>
              if (successorNodeRef == context.self) {
                /*
                 * This is the first node to join the ring
                 */
                context.log.info(s"${context.self.path}\t:\tI am the first node to join the ring")
                val newSlotToHash = mutable.Map[Int, Int]()
                val newHashToRef = mutable.Map[Int, ActorRef[Node.Command]]()
                (0 until m).foreach(i => {
                  newSlotToHash += (i -> hashValue)
                  newHashToRef += (hashValue -> context.self)
                })
                context.log.info(s"${context.self.path}\t:\tInitialized finger tables\t:\tslotToHash = $newSlotToHash\thashToRef = $newHashToRef")
                process(predecessorsUpdated=true, movies, m, newSlotToHash, newHashToRef, successorNodeRef, predecessorNodeRef)
              }

              else {
                /*
                 * There is at least 1 other node already in the ring
                 * Start the finger table initialization process using the successor node for each value of i (0...m-1)
                 * asynchronously
                 */
                context.log.info(s"${context.self.path}\t:\tI am not the first node to join the ring")
                context.log.info(s"${context.self.path}\t:\tAsking successor node $successorNodeRef for its state...")
                implicit val timeout: Timeout = 5.seconds
                val newSlotToHash = mutable.Map[Int, Int]()
                val newHashToRef = mutable.Map[Int, ActorRef[Node.Command]]()
                var newPredecessor: ActorRef[Node.Command] = null

                context.ask(successorNodeRef, GetActorState) {
                  case x @ Success(ActorStateResponse(_, _, successorHashValue, successorPredecessor)) =>
                    context.log.info(s"${context.self.path}\t:\tHash value = $hashValue\t:\tStarting to initialize finger table...")

                    newSlotToHash += (0 -> successorHashValue)
                    newHashToRef += (successorHashValue -> successorNodeRef)
                    newPredecessor = successorPredecessor

                    context.log.info(s"${context.self.path}\t:\tNew predecessor found is $newPredecessor")
                    context.log.info(s"${context.self.path}\t:\tletting successor node $successorNodeRef know that I am its new predecessor")
                    successorNodeRef ! SetPredecessor(context.self)

                    context.log.info(s"${context.self.path}\t:\t(i = 0) => (fingerNode = $successorNodeRef\t;\thash = $successorHashValue)")
                    successorNodeRef ! FindSuccessor(1, ((hashValue + Math.pow(2, 1).round) % Math.pow(2, m)).toInt,
                      successorNodeRef, context.self, hashValue)

                    x.value
                  case Failure(_) => ActorStateResponseError
                }

                process(predecessorsUpdated, movies, m, newSlotToHash, newHashToRef, successorNodeRef, predecessorNodeRef)
              }

            /*
            * The actor node has to send it's current state to the parent node
            */
            case Parent.DumpActorState(replyTo) =>
              replyTo ! ActorState(context.self, getCurrentState(hashValue, slotToHash, movies))
              Behaviors.same

            /*
            * The actor node is requested for it's finger table
            */
            case GetFingerTable(replyTo) =>
              context.log.info(s"${context.self.path} : got request for finger table from $replyTo")
              replyTo ! FingerTableResponse(slotToHash, hashToRef)
              Behaviors.same

             /*
             * The actor node has received a finger table from another node
             */
            case FingerTableResponse(slotToHash, hashToRef) =>
              context.log.info(s"${context.self.path} : Received this finger table - ${slotToHash.toString()} and ${hashToRef.toString()}")
              Behaviors.same

              /*
              * Finger table from another node was not received due to a problem
              */
            case FingerTableResponseError =>
              context.log.error(s"${context.self.path} : could not receive finger table")
              Behaviors.same

              /*
              * The actor is requested for it's hash value from another node
              */
            case GetHashValue(replyTo) =>
              context.log.info(s"${context.self.path} : got request for hash value from $replyTo")
              replyTo ! HashResponse(hashValue)
              Behaviors.same

              /*
              * The actor has received hash value from another node
              */
            case HashResponse(hashValue) =>
              context.log.info(s"${context.self.path} : received this hash value - $hashValue")
              Behaviors.same

              /*
              * There was a problem with getting the hash value from another node
              */
            case HashResponseError =>
              context.log.error(s"${context.self.path} : could not receive hash value")
              Behaviors.same

              /*
              * The actor is asked for it's predecessor actor node
              */
            case GetPredecessor(replyTo) =>
              context.log.info(s"${context.self.path} : got request for predecessor from $replyTo")
              replyTo ! PredecessorResponse(predecessor)
              Behaviors.same

              /*
              * The actor has received a predecessor actor reference from another actor
              */
            case PredecessorResponse(predecessor) =>
              context.log.info(s"${context.self.path} : got this predecessor value - $predecessor")
              Behaviors.same

              /*
              * The actor has to update it's predecessor reference to a new node which has joined the ring
              */
            case SetPredecessor(newPredecessor) =>
              context.log.info(s"${context.self.path}\t:\tgot $newPredecessor as my new predecessor. Resetting $predecessor to $newPredecessor")
              process(predecessorsUpdated, movies, m, slotToHash, hashToRef, successor, newPredecessor)

              /*
              * The actor has to update its finger table, and possibly successor reference if the new node happens to be
              * its new successor
              */
            case UpdateFingerTableAndSuccessor(newSuccessor, i, newHash, newHashRef) =>
              context.log.info(s"${context.self.path}\t:\tgot new finger tables:\tslotToHash = ${slotToHash+(i -> newHash)}\thashToRef = " +
                s"${hashToRef+(newHash -> newHashRef)}")
              newSuccessor match {
                case null => process(predecessorsUpdated, movies, m, slotToHash+(i -> newHash), hashToRef+(newHash -> newHashRef),
                  successor, predecessor)
                case _ =>
                  context.log.info(s"${context.self.path}\t:\tgot $newSuccessor as my new successor. Resetting successor " +
                    s"$successor to $newSuccessor")
                  process(predecessorsUpdated, movies, m, slotToHash+(i -> newHash), hashToRef+(newHash -> newHashRef), newSuccessor, predecessor)
              }

              /*
              * The actor is requested for its state (finger table maps, hash value and predecessor reference)
              */
            case GetActorState(replyTo) =>
              context.log.info(s"${context.self.path} : got request for actor state - $replyTo")
              replyTo ! ActorStateResponse(slotToHash, hashToRef, hashValue, predecessor)
              Behaviors.same

              /*
              * The actor has received another actor's state
              */
            case ActorStateResponse(_, _, _, _) =>
              context.log.info(s"${context.self.path} : got actor state response")
              Behaviors.same

              /*
              * There was a problem receiving another actor's state
              */
            case ActorStateResponseError =>
              context.log.error(s"${context.self.path} : could not get actor state response")
              Behaviors.same

              /*
              * The actor has received a query to find the successor for a data to be stored
              */
            case FindNodeForStoringData(data, srcRouteRef) =>
              val id = getSignedHash(m, data.name)
              context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tGot request for finding a node to store data with key $id")
              findSuccessorToStoreData(data, id, srcRouteRef, hashValue, slotToHash(0), context, slotToHash, hashToRef)
              Behaviors.same

              /*
              * The actor is requested to store data as it is found to be the successor for the data
              */
            case StoreData(data, id, srcRouteRef) =>
              context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tGot request to store data with key $id")
              srcRouteRef ! DataStorageResponseSuccess(s"Movie '${data.name}' uploaded successfully")
              process(predecessorsUpdated, movies+data, m, slotToHash, hashToRef, successor, predecessor)

              /*
              * The actor has received a query to find the successor of an existing data(movie)
              */
            case FindSuccessorToFindData(name, srcRouteRef) =>
              findSuccessorToFindData(name, getSignedHash(m, name), srcRouteRef, hashValue, slotToHash(0), context, slotToHash, hashToRef)
              Behaviors.same

              /*
              * The actor is requested to find the predecessor for a given key
              */
            case FindPredecessorToFindData(name, id, replyTo, srcRouteRef) =>
              findPredecessorToFindData(name, id, replyTo, srcRouteRef, hashValue, slotToHash(0), context, slotToHash, hashToRef)
              Behaviors.same

              /*
              * The actor is notified here that the successor has been found for a requested key, this actor being the
              * source of the request.
              * The actor will request the successor to find the given key
              */
            case SuccessorFoundForData(name, id, successorRef, srcRouteRef) =>
              successorRef ! GetData(name, id, srcRouteRef)
              Behaviors.same

            case SuccessorNotFound(name, _, srcRouteRef) =>
              srcRouteRef ! DataResponseFailed(s"Could not find movie '$name'")
              Behaviors.same

              /*
              * The actor has been requested for a data (movie) that it may or may not have stored.
              */
            case GetData(name, id, srcRouteRef) =>
              context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tgot request to search for data key $id stored with me. Searching...")
              val data = movies.find(_.name == name)
              data match {
                case None =>
                  context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tRequested movie '$name' could not be found")
                  srcRouteRef ! DataResponseFailed(s"Could not find movie '$name'")
                case _ =>
                  context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tFound movie '$name' with me")
                  srcRouteRef ! DataResponseSuccess(data)
              }
              Behaviors.same

              /*
              * The actor is requested to provide the set of all movies it has
              */
            case GetAllData(replyTo) =>
              replyTo ! AllData(movies)
              Behaviors.same

              /*
              * The actor is requested to find the successor for a given key as a result of a new node joining the ring
              */
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

              /*
              * The actor is requested find the predecessor of a given key as a result of a new node joining the ring
              */
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

              /*
              * The actor is notified about the successor that has been found for a key that this actor had requested
              * as a result of a new node joining the ring
              */
            case SuccessorFound(i, id, successor, successorHashValue, newNodeRef) =>
              context.log.info(s"${context.self.path} : node $successor with hash $successorHashValue found as " +
                s"successor of key $id for $newNodeRef")
              newNodeRef ! NewSuccessorResponse(i, successor, successorHashValue)
              Behaviors.same

              /*
              * This is the new node which is receiving its requested finger reference to fill up its finger table
              * If the whole finger table has been computed, then this new node will initialize the process of updating
              * all it's predecessor's finger table, otherwise it will repeat the same procedure to compute successive
              * finger entries
              */
            case NewSuccessorResponse(i, newSuccessor, newSuccessorHashValue) =>
              if (i < m) {
                context.log.info(s"${context.self.path}\t:\t(i = $i) => (fingerNode = $newSuccessor\t;\thash = $newSuccessorHashValue)")
                successor ! FindSuccessor(i+1, ((hashValue+Math.pow(2, i+1).round) % Math.pow(2, m)).toInt, successor, context.self, hashValue)
                process(predecessorsUpdated, movies, m, slotToHash+(i->newSuccessorHashValue),
                  hashToRef+(newSuccessorHashValue->newSuccessor), successor, predecessor)
              }
              else {
                context.log.info(s"${context.self.path}\t:\tInitialized finger tables: \nslotToHash - $slotToHash\nhashToRef - $hashToRef")
                context.log.info(s"${context.self.path}\t:\tFinding my predecessor 0 to update")
                findPredecessorToUpdate(0, getSignedHashOfRingSlotNumber(m, (hashValue-Math.pow(2, 0).round).toInt),
                  context.self, hashValue, slotToHash(0), context, slotToHash, hashToRef)
                Behaviors.same
              }

            case NewSuccessorResponseError => Behaviors.same

              /*
              * This actor has been identified as a predecessor of a new node which has joined the ring, and should check if it's
              * finger table entry is to be updated or not.
              */
            case UpdateFingerTable(newNode, newNodeHashValue, i) =>
              if (slotToHash.contains(i)) {
                context.log.info(s"${context.self.path}\t:\tGot request to update finger $i in my finger table if required")
                updateFingerTable(newNode, newNodeHashValue, i, hashValue, slotToHash(i), context, slotToHash, hashToRef, predecessor)
              }
              Behaviors.same

              /*
              * This actor has received a request to find a predecessor node of a key to update it's finger table
              */
            case FindPredecessorToUpdate(i, id, replyTo) =>
              context.log.info(s"${context.self.path}\t:\tGot request to find successor of key $id")
              findPredecessorToUpdate(i, id, replyTo, hashValue, slotToHash(0), context, slotToHash, hashToRef)
              Behaviors.same

              /*
              * This new node has received its predecessor node reference.
              * If the predecessor is not itself, then it will ask that predecessor to update its finger table.
              * If all predecessors have been notified then the new node has successfully joined the ring, otherwise
              * it will continue finding the successive predecessor nodes to notify of the update procedure.
              */
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
                Behaviors.same
              }
              else {
                context.log.info(s"${context.self.path}\t:\tFinished telling all my predecessors to update their finger tables")
                process(predecessorsUpdated=true, movies, m, slotToHash, hashToRef, successor, predecessor)
              }

              /*
              * This actor has received a request asking if it has finished notifying all its predecessor nodes or not.
              */
            case AllPredecessorsUpdated(replyTo) =>
              replyTo ! AllPredecessorsUpdatedResponse(predecessorsUpdated)
              Behaviors.same
          }
        })
      }

    /**
     * This function will find initiate the search procedure to find the successor of a key (movie) that is to be stored.
     * @param data The movie to be stored
     * @param id The hash of the movie to be stored
     * @param srcRouteRef Reference of the actor created by "ask" sent to the parent actor as a result of the http request
     * @param hashValue Hash value of this actor node
     * @param successorHashValue Hash value of successor of this actor node
     * @param slotToHash Finger table map - (i -> hashValue of finger)
     * @param hashToRef Finger table map - (hashValue of finger -> actor ref)
     * @param context Execution context of this actor
     */
    def findSuccessorToStoreData(data: Data, id: Int, srcRouteRef: ActorRef[DataActionResponse],
                                hashValue: Int, successorHashValue: Int, context: ActorContext[Command],
                                slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command]  = {
      if (hashValue == successorHashValue || id == hashValue) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tI am storing data with key $id")
        context.self ! StoreData(data, id, srcRouteRef)
      }
      else if (isInLeftOpenInterval(id, hashValue, successorHashValue)) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tStoring data key $id in my successor node with key $successorHashValue")
        hashToRef(slotToHash(0)) ! StoreData(data, id, srcRouteRef)
      }
      else {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tFinding closest preceding node to store data with key $id")
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context, slotToHash, hashToRef)
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tClosest preceding node found is $closestPrecedingNodeRef")
        if (closestPrecedingNodeRef == context.self) {
          context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tStoring data key $id in myself")
          closestPrecedingNodeRef ! StoreData(data, id, srcRouteRef)
        }
        else {
          context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tSending search of node for data key $id to $closestPrecedingNodeRef")
          closestPrecedingNodeRef ! FindNodeForStoringData(data, srcRouteRef)
        }
      }
      Behaviors.same
    }

    /**
     * This function will initiate the procedure of finding successor of a key (movie) already stored in the ring
     * @param name Name of the movie requested
     * @param id Hash of the movie requested
     * @param srcRouteRef Reference of the actor created by "ask" sent to the parent actor as a result of the http request
     * @param hashValue Hash value of this actor node
     * @param successorHashValue Hash value of successor of this actor node
     * @param slotToHash Finger table map - (i -> hashValue of finger)
     * @param hashToRef Finger table map - (hashValue of finger -> actor ref)
     * @param context Execution context of this actor
     */
    def findSuccessorToFindData(name: String, id: Int, srcRouteRef: ActorRef[DataActionResponse], hashValue: Int, successorHashValue: Int,
                                context: ActorContext[Command], slotToHash: mutable.Map[Int, Int],
                                hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] =
      findPredecessorToFindData(name, id, context.self, srcRouteRef, hashValue, successorHashValue, context, slotToHash, hashToRef)

    /**
     * This function will find the predecessor of the requested key (movie) already stored in the ring
     * @param replyTo The actor which is the source of the request
     * @param hashValue Hash value of this actor node
     * @param successorHashValue Hash value of successor of this actor node
     * @param slotToHash Finger table map - (i -> hashValue of finger)
     * @param hashToRef Finger table map - (hashValue of finger -> actor ref)
     * @param context Execution context of this actor
     * @param name The name of the movie being searched
     * @param id Hash of the movie being searched
     */
    def findPredecessorToFindData(name: String, id: Int, replyTo: ActorRef[Command], srcRouteRef: ActorRef[DataActionResponse],
                                  hashValue: Int, successorHashValue: Int, context: ActorContext[Command],
                                  slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] = {
      context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tGot request for processing data key $id")
      if (id == hashValue) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tI have the data for key $id. Returning myself.")
        replyTo ! SuccessorFoundForData(name, id, context.self, srcRouteRef)
      }
      else if (hashValue != successorHashValue && isInLeftOpenInterval(id, hashValue, successorHashValue)) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tSuccessor with key $successorHashValue should have the key $id. " +
          s"Returning ${hashToRef(slotToHash(0))}")
        replyTo ! SuccessorFoundForData(name, id, hashToRef(slotToHash(0)), srcRouteRef)
      }
      else if (hashValue != successorHashValue) {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tFinding the closest preceding node to key $id")
        val closestPrecedingNodeRef = findClosestPrecedingFinger(id, hashValue, context, slotToHash, hashToRef)

        if (closestPrecedingNodeRef == context.self) {
          context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tClosest preceding node found is myself. Key $id not found")
          replyTo ! SuccessorNotFound(name, id, srcRouteRef)
        }

        else {
          context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tClosest preceding node found is $closestPrecedingNodeRef")
          context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tSending search to node $closestPrecedingNodeRef")
          closestPrecedingNodeRef ! FindPredecessorToFindData(name, id, replyTo, srcRouteRef)
        }
      }
      else {
        context.log.info(s"${context.self.path}\thash=($hashValue)\t:\tCould not find node for data key $id")
        replyTo ! SuccessorNotFound(name, id, srcRouteRef)
      }
      Behaviors.same
    }

    /**
     * This function will find the most immediately preceding node of the requested key using this actor's finger table maps
     * @param id Key whose most immediate preceding node is to be found
     * @param hashValue Hash value of this actor
     * @param context The execution context of this actor
     * @param slotToHash Finger table map (i -> hash of finger)
     * @param hashToRef Finger table map (hash of finger -> actor ref)
     */
    def findClosestPrecedingFinger(id: Int, hashValue: Int, context: ActorContext[Command],
                                   slotToHash: mutable.Map[Int, Int], hashToRef: mutable.Map[Int, ActorRef[Command]]): ActorRef[Command] = {
      ((m-1) to 0 by -1).foreach(i => {
        if (slotToHash.contains(i) && isInOpenInterval(slotToHash(i), hashValue, id)) return hashToRef(slotToHash(i))
      })
      context.self
    }

    /**
     * This function will initiate the procedure to find the successor (finger) of a key requested by a new node which
     * has joined the ring.
     * @param i Current index of the finger table whose corresponding finger is to be found
     * @param id The requested key whose successor is to be found
     * @param newNodeRef The actor reference of the new node
     * @param newNodeHashValue Hash value of the new node
     * @param hashValue Hash value of this actor
     * @param successorHashValue Hash value of this actor node's successor node
     * @param context Execution context of this actor
     * @param slotToHash Finger table map of this actor (i -> finger hash)
     * @param hashToRef Finger table map (finger hash -> actor reference)
     */
    def findSuccessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command], newNodeHashValue: Int,
                      hashValue: Int, successorHashValue: Int, context: ActorContext[Command], slotToHash: mutable.Map[Int, Int],
                      hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] =
      findPredecessor(i: Int, id, replyTo, newNodeRef, newNodeHashValue, hashValue, successorHashValue, context, slotToHash, hashToRef)

    /**
     * This function will find the predecessor of a key requested by a new node
     * @param i Current index of the finger table whose corresponding finger is to be found
     * @param id The requested key whose successor is to be found
     * @param newNodeRef The actor reference of the new node
     * @param newNodeHashValue Hash value of the new node
     * @param hashValue Hash value of this actor
     * @param successorHashValue Hash value of this actor node's successor node
     * @param context Execution context of this actor
     * @param slotToHash Finger table map of this actor (i -> finger hash)
     * @param hashToRef Finger table map (finger hash -> actor reference)
     */
    def findPredecessor(i: Int, id: Int, replyTo: ActorRef[Command], newNodeRef: ActorRef[Command], newNodeHashValue: Int,
                        hashValue: Int, successorHashValue: Int,
                        context: ActorContext[Command], slotToHash: mutable.Map[Int, Int],
                        hashToRef: mutable.Map[Int, ActorRef[Command]]): Behavior[Command] = {
      context.log.info(s"${context.self.path}\t:\tid = $id\thash = $hashValue\tsuccessorHash=$successorHashValue")
      /*
       * Check if requested key successor is between myself and new node's position (clockwise direction),
       * if it is then new node itself is the successor otherwise if it is between new node and myself then I am the successor
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

    /**
     * This function will find the predecessor of a requested key whose finger table should be (possibly) updated
     * @param i Current index of the finger table whose corresponding finger is to be found
     * @param id The requested key whose successor is to be found
     * @param hashValue Hash value of this actor
     * @param successorHashValue Hash value of this actor node's successor node
     * @param context Execution context of this actor
     * @param slotToHash Finger table map of this actor (i -> finger hash)
     * @param hashToRef Finger table map (finger hash -> actor reference)
     */
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

        if (closestPrecedingNodeRef != context.self) {
          context.log.info(s"${context.self.path}\t:\tcould not find successor of key $id. Sending search to $closestPrecedingNodeRef")
          closestPrecedingNodeRef ! FindPredecessorToUpdate(i, id, replyTo)
        }
      }
      Behaviors.same
    }

    /**
     * This function will check if the new node succeeds the current ith finger of this actor node, and if it does
     * then this actor will update its ith entry in the finger table map to the new node's hash value, by way of sending
     * itself an UpdateFingerTableAndSuccessor(...) message.
     * If i = 0 then this actor will also update its successor reference to be the new node's reference
     * If new node doesn't succeed its ith finger then it will do nothing
     * @param i Current index of the finger table whose corresponding finger is to be found
     * @param newNode new node reference
     * @param newNodeHashValue Hash value of the new node
     * @param predecessorHashValue Hash value of this actor
     * @param predecessorSuccessorHashValue Hash value of successor of this actor
     * @param context Execution context of this actor
     * @param slotToHash Finger table map of this actor (i -> finger hash)
     * @param hashToRef Finger table map (finger hash -> actor reference)
     */
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

    /**
     * This function checks if a given key is in the interval (left, right] i.e. id > left and id <= right
     * in a circular manner.
     * @param id Key to be checked
     * @param hash left value of the interval which is exclusive
     * @param successorHash Right value of the interval which is inclusive
     */
    def isInLeftOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean =
      if (successorHash == hash || id == successorHash) true
      else if (successorHash > hash) id > hash && id <= successorHash
      else (id > hash) || (id >= 0 && id < successorHash)

    /**
     * This function checks if a given key is in the interval [left, right) i.e. id >= left and id < right
     * in a circular manner.
     * @param id Key to be checked
     * @param hash left value of the interval which is inclusive
     * @param successorHash Right value of the interval which is exclusive
     */
    def isInRightOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean = {
      if (successorHash == hash) true
      else if (successorHash > hash) id >= hash && id < successorHash
      else (id >= hash) || (id >= 0 && id < successorHash)
    }

    /**
     * This function checks if a given key lies in the interval (left, right) i.e. id > left and id < right
     * in a circular manner.
     * @param id Key to be checked
     * @param hash left value of the interval which is exclusive
     * @param successorHash Right value of the interval which is exclusive
     */
    def isInOpenInterval(id: Int, hash: Int, successorHash: Int): Boolean = {
      if (successorHash == hash) true
      else if (successorHash > hash) id > hash && id < successorHash
      else (id > hash) || (id >= 0 && id < successorHash)
    }

    /**
     * This function will wrap this actor's state(finger table map, hash value and all movies) in a StateDump(...) message
     * and return it.
     * @param hashValue Hash value of this actor node
     * @param slotToHash Finger table map (i -> hash of finger)
     * @param movies Set of movies currently stored by this actor node
     */
    def getCurrentState(hashValue: Int, slotToHash: mutable.Map[Int, Int], movies: Set[Data]): StateToDump =
      StateToDump(Map("hash" -> hashValue), List(slotToHash), movies)

    process(predecessorsUpdated=false, Set.empty, m, slotToHash, hashToRef, successor, predecessor)
  }
}
