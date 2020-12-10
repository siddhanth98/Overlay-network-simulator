package com.can

import akka.actor.Cancellable
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, Terminated}
import com.can.Parent.FindAnotherNodeForQuery
import com.utils.UnsignedInt

import java.nio.ByteBuffer
import java.security.MessageDigest
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{DurationDouble, DurationInt}
import scala.math.BigInt.javaBigInteger2bigInt
import scala.util.Random

/**
 * This actor represents a node in the CAN
 */
object Node {
  trait Command
  trait DataActionResponse extends Command

  final case class GetCoordinates(replyTo: ActorRef[NodeCoordinates]) extends Command
  final case class NodeCoordinates(coordinates: Array[Array[Double]]) extends Command

  /**
   * Message sent by a newly joined node to a random node already in the CAN
   */
  final case class Join(replyTo: ActorRef[Command], splitAxis: Character) extends Command

  /**
   * Set of messages for periodically notifying neighbours with current neighbour set
   */
  final case object NotifyMyNeighbours extends Command
  final case class NotifyNeighboursWithNeighbourMap(neighbourRef: ActorRef[Command],
                                                    neighbours: Map[ActorRef[Command], Array[Array[Double]]]) extends Command

  /**
   * Message received by new node as part of the split join process
   */
  final case class SplitResponse(newCoordinates: Array[Array[Double]], neighbours: Map[ActorRef[Command], Array[Array[Double]]]) extends Command

  /**
   * Message sent by new node to its neighbours to update their neighbour maps
   */
  final case class NotifyNeighboursToUpdateNeighbourMaps(newNeighbourRef: ActorRef[Command], newCoordinates: Array[Array[Double]]) extends Command

  /**
   * Message sent by existing split node to its existing neighbours to update their neighbour maps
   */
  final case class UpdateMyNeighbourCoordinates(neighbourRef: ActorRef[Command], coordinates: Array[Array[Double]]) extends Command

  /**
   * This message tells (previously-overlapping) non-overlapping nodes to remove this node from their neighbour map
   */
  final case class RemoveNeighbours(neighbourRef: ActorRef[Command]) extends Command

  /**
   * Data to be stored at the node
   */
  final case class Movie(name: String, size: Int, genre: String)
  final case object Replicate extends Command
  final case class SendReplica(movie: Set[Movie], ownerNode: ActorRef[Command]) extends Command

  /**
   * Set of messages for handling movie storage / retrieval requests and responses
   */
  final case class FindNodeForStoringData(parentRef: ActorRef[Parent.Command], replyTo: ActorRef[DataActionResponse], name: String, size: Int, genre: String,
                                          endX: Int, endY: Int, requesterNode: ActorRef[Command] = null) extends Command
  final case class DataStorageResponseSuccess(description: String) extends DataActionResponse
  final case class DataStorageResponseFailure(description: String) extends DataActionResponse

  final case class FindSuccessorForFindingData(parentRef: ActorRef[Parent.Command], replyTo: ActorRef[DataActionResponse], name: String, endX: Int, endY: Int,
                                               requesterNode: ActorRef[Command] = null) extends Command
  final case class DataResponseSuccess(movie: Option[Movie]) extends DataActionResponse
  final case class DataResponseFailed(description: String) extends DataActionResponse

  /**
   * Set of messages for the takeover mechanism
   */
  final case class Takeover(failedNode: ActorRef[Command], area: Double, requestingNodeId: Int, requestingNodeRef: ActorRef[Command])
    extends Command
  final case class TakeoverAck(failedNode: ActorRef[Command], node: ActorRef[Command]) extends Command
  final case class TakeoverNack(failedNode: ActorRef[Command]) extends Command
  final case class TakeoverFinal(failedNode: ActorRef[Command], takeoverNode: ActorRef[Command],
                                 takeoverNodeCoordinates: Array[Array[Double]]) extends Command
  final case class TakeoverMyZone(takeoverNode: ActorRef[Command], takeoverNodeCoordinates: Array[Array[Double]]) extends Command
  final case class TakeoverFailedNodeZone(requester: ActorRef[Command], failedNode: ActorRef[Command],
                                          failedNodeCoordinates: Array[Array[Double]]) extends Command

  /**
   * When the parent actor sends this message to a child node actor, it will stop
   */
  final case object Stop extends Command with Parent.Command

  final case class State(coordinates_2D: String, Movies: Set[Movie], Replica: Map[ActorRef[Command], Set[Movie]])

  def md5(s: String): Array[Byte] = MessageDigest.getInstance("MD5").digest(s.getBytes)

  def getSignedHash(m: Int, s: String): Int = (UnsignedInt(ByteBuffer.wrap(md5(s)).getInt).bigIntegerValue % m).intValue()

  /**
   * Constructor for 1st node to join the CAN
   */
  def apply(nodeId: Int, endX: Int, endY: Int, replicationPeriod: Int): Behavior[Command] = Behaviors.setup{context =>
    context.log.info(s"${context.self.path}\t:\tI am the first node in the CAN")
    Behaviors.withTimers{timer =>
      timer.startTimerWithFixedDelay(NotifyMyNeighbours, 3.seconds)
      timer.startTimerWithFixedDelay(Replicate, replicationPeriod.seconds)
      process(nodeId, Set.empty, Map.empty, Array(Array(1, endX), Array(1, endY)), Map.empty, Map.empty)
    }
  }

  /**
   * Constructor for successive node to join the CAN
   */
  def apply(nodeId: Int, existingNodeRef: ActorRef[Command], splitAxis: Character, replicationPeriod: Int):
  Behavior[Command] = Behaviors.setup{context =>
    context.log.info(s"${context.self.path}\t:\tI am not the first node to join the CAN. Got existing node $existingNodeRef")
    context.log.info(s"${context.self.path}\t:\tSending join message to $existingNodeRef")
    Behaviors.withTimers{timer =>
      timer.startTimerWithFixedDelay(NotifyMyNeighbours, 3.seconds)
      timer.startTimerWithFixedDelay(Replicate, replicationPeriod.seconds)
      existingNodeRef ! Join(context.self, splitAxis)
      process(nodeId, Set.empty, Map.empty, Array[Array[Double]](), Map.empty, Map.empty)
    }
  }

  /**
   * Defines the behavior of the node actor
   */
  def process(nodeId: Int, movies: Set[Movie], replicaSet: Map[ActorRef[Command], Set[Movie]], coordinates: Array[Array[Double]],
              neighbours: Map[ActorRef[Command], Array[Array[Double]]],
              transitiveNeighbourMap: Map[ActorRef[Command], Map[ActorRef[Command], Array[Array[Double]]]], takeOverMode: Boolean = false,
              schedulerInstance: Cancellable = null, takeOverAckedNodes: Map[ActorRef[Command], Array[Array[Double]]] = Map.empty):
  Behavior[Command] = Behaviors.receive[Command] {

    case (context, GetCoordinates(replyTo)) =>
      replyTo ! NodeCoordinates(coordinates)
      Behaviors.same

    case (context, Stop) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived stop message from parent.")
      Behaviors.stopped

    case (context, Parent.DumpNodeState(parentRef)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSending state to parent...")
      parentRef ! Parent.NodeState(context.self, getState(coordinates, movies, replicaSet))
      Behaviors.same

        /*
         * Notifies neighbours about this node's current neighbours
         */
    case (context, NotifyMyNeighbours) =>
//      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tNotifying neighbours...")

      neighbours.keySet.foreach(k => k ! NotifyNeighboursWithNeighbourMap(context.self, neighbours))
      Behaviors.same

      /*
      * Send movie replica to store with neighbours
      */
    case (context, Replicate) =>
//      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSending replica to my neighbours...")
      neighbours.keySet.foreach(n => n ! SendReplica(movies, context.self))
      Behaviors.same

    case (context, SendReplica(replica, nodeRef)) =>
//      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived replica set from $nodeRef")
      process(nodeId, movies, replicaSet+(nodeRef->replica), coordinates, neighbours, transitiveNeighbourMap)
      /*
      * New node gets its new coordinates and neighbour coordinates from split node
      */
    case (context, SplitResponse(newCoordinates, neighboursMap)) =>
      context.log.info(s"${context.self.path}\t:\tReceived split response message with new coordinates - [${printCoordinates(newCoordinates)}] " +
        s"and new neighbours refs - ${printNeighboursAndCoordinates(neighboursMap)}")
      context.log.info(s"${context.self.path}\t:\tWatching neighbours and notifying neighbours to update their neighbour maps...")
      neighboursMap.keySet.foreach(k => {
        context.watch(k)
        k ! NotifyNeighboursToUpdateNeighbourMaps(context.self, newCoordinates)
      })
      process(nodeId, movies, replicaSet, newCoordinates, neighboursMap, transitiveNeighbourMap)

      /*
      * A new node has sent a split join request to this node
      */
    case (context, Join(replyTo, splitAxis)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived join message from $replyTo with split axis $splitAxis")
      val startX = coordinates(0)(0)
      val endX = coordinates(0)(1)
      val startY = coordinates(1)(0)
      val endY = coordinates(1)(1)
      if (splitAxis == 'X') {
        /* Split along the x axis vertically, send new node its new right zone coordinates and neighbours */
        val newNodeCoordinates = Array(Array((startX + endX)/2D, endX), Array(startY, endY))
        val newNodeNeighbourSet = findNeighboursForOverlappingCoordinates(neighbours, (startX + endX)/2D, endX, startY, endY)
        val newNodeNeighbourMap = getNeighbourCoordinatesFromNewNeighbourSet(newNodeNeighbourSet, neighbours)

        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSplitting myself and giving coordinates [${printCoordinates(newNodeCoordinates)}] to" +
          s"$replyTo, along with the following neighbours and their respective coordinates - ${printNeighboursAndCoordinates(newNodeNeighbourMap)}, including myself")
        val mySplitCoordinates = splitMyCoordinates(splitAxis, coordinates)

        replyTo ! SplitResponse(newNodeCoordinates, newNodeNeighbourMap + (context.self -> mySplitCoordinates))

        /* Update existing node's neighbours after split and notify current neighbours */

        val myUpdatedNeighbours = getNeighbourCoordinatesFromNewNeighbourSet(
          findNeighboursForOverlappingCoordinates(neighbours, startX, (startX+endX-1)/2D, startY, endY), neighbours
        )
        neighbours.keySet.diff(myUpdatedNeighbours.keySet).foreach(o => {
          context.unwatch(o)
          o ! RemoveNeighbours(context.self)
        })
        myUpdatedNeighbours.keySet.foreach(k => k ! NotifyNeighboursToUpdateNeighbourMaps(context.self, mySplitCoordinates))
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy split coordinates are now - [${printCoordinates(mySplitCoordinates)}] and new neighbours" +
          s"are - ${printNeighboursAndCoordinates(myUpdatedNeighbours)}")
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tPutting $replyTo as my new neighbour")

        (myUpdatedNeighbours.keySet+replyTo).foreach(n => context.watch(n))
        process(nodeId, movies, replicaSet, mySplitCoordinates, myUpdatedNeighbours+(replyTo -> newNodeCoordinates), transitiveNeighbourMap)
      }

      else {
        /* Split along the y axis horizontally, send new node its new upper zone coordinates and neighbours */
        val newNodeCoordinates = Array(Array(startX, endX), Array((startY+endY)/2D, endY))
        val newNodeNeighbourSet = findNeighboursForOverlappingCoordinates(neighbours, startX, endX, (startY+endY)/2D, endY)
        val newNodeNeighbourMap = getNeighbourCoordinatesFromNewNeighbourSet(newNodeNeighbourSet, neighbours)

        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSplitting myself and giving coordinates [${printCoordinates(newNodeCoordinates)}] to" +
          s"$replyTo, along with the following neighbours and their respective coordinates - ${printNeighboursAndCoordinates(newNodeNeighbourMap)}, including myself")

        val mySplitCoordinates = splitMyCoordinates(splitAxis, coordinates)
        replyTo ! SplitResponse(newNodeCoordinates, newNodeNeighbourMap+(context.self -> mySplitCoordinates))

        /* Update existing node's neighbours after split */
        val myUpdatedNeighbours = getNeighbourCoordinatesFromNewNeighbourSet(
          findNeighboursForOverlappingCoordinates(neighbours, startX, endX, startY, (startY+endY-1)/2D), neighbours
        )
        neighbours.keySet.diff(myUpdatedNeighbours.keySet).foreach(o => {
          context.unwatch(o)
          o ! RemoveNeighbours(context.self)
        })
        myUpdatedNeighbours.keySet.foreach(k => k ! NotifyNeighboursToUpdateNeighbourMaps(context.self, mySplitCoordinates))
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy split coordinates are now - [${printCoordinates(mySplitCoordinates)}] and new neighbours" +
          s" are - ${printNeighboursAndCoordinates(myUpdatedNeighbours)}")
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tPutting $replyTo as my new neighbour")

        (myUpdatedNeighbours.keySet+replyTo).foreach(n => context.watch(n))
        process(nodeId, movies, replicaSet, mySplitCoordinates, myUpdatedNeighbours+(replyTo -> newNodeCoordinates), transitiveNeighbourMap)
      }

    case (context, Takeover(failedNode, area, requestingNodeId, requestingNodeRef)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tGot request for takeover of $failedNode from " +
        s"transitive neighbour $requestingNodeRef")

      if (!takeOverMode && (area < findMyArea(coordinates) || (requestingNodeId < nodeId && area == findMyArea(coordinates)))) {
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tI am not in takeover mode so cancelling my " +
          s"takeover attempt of $failedNode")
        requestingNodeRef ! TakeoverAck(failedNode, context.self)
        process(nodeId, movies, replicaSet, coordinates, neighbours, transitiveNeighbourMap)
      }
      else {
        val myArea = findMyArea(coordinates)
        if (area < myArea || (requestingNodeId < nodeId && area == myArea)) {
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tcancelling my takeover attempt of $failedNode")
          if (schedulerInstance != null) schedulerInstance.cancel()
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSending takeover ACK of $failedNode to $requestingNodeRef")
          requestingNodeRef ! TakeoverAck(failedNode, context.self)
          process(nodeId, movies, replicaSet, coordinates, neighbours, transitiveNeighbourMap)
        }
        else {
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tsending takeover NACK of $failedNode to $requestingNodeRef")
          requestingNodeRef ! TakeoverNack(failedNode)
          process(nodeId, movies, replicaSet, coordinates, neighbours, transitiveNeighbourMap, takeOverMode=true, schedulerInstance)
        }
      }

    case (context, TakeoverAck(failedNode, respondingNode)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\treceived takeover ACK of $failedNode from $respondingNode")
      if (transitiveNeighbourMap(failedNode).contains(respondingNode) &&
        transitiveNeighbourMap(failedNode).size == 2) {
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tI have received all ACKs for taking over " +
          s"zone of $failedNode.")
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tComputing new coordinates and " +
          s"putting neighbours $failedNode in my neighbours map")
        val isMergeable: Boolean = isPerfectMerge(coordinates, neighbours(failedNode))

        if (!isMergeable) {
          /* Find a perfect mergeable neighbour and tell it to takeover your zone. Take failed node's zone and include its neighbours
          *  in your neighbour map.
          *  Remove all current neighbours of yours which do not overlap with your new coordinates.
          *  Perfect neighbour should include your old neighbours in its neighbour map.
          * */
          val perfectlyMergeableNeighbour = findPerfectMergeableNeighbour(coordinates, neighbours)

          if (perfectlyMergeableNeighbour == null) {
            context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tI do not have a perfectly mergeable neighbour. " +
              s"Finding one from neighbours of $failedNode.")
            val perfectTakeoverNode = findPerfectMergeableNeighbour(neighbours(failedNode), transitiveNeighbourMap(failedNode))

            context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tAsking $perfectTakeoverNode to takeover zone of $failedNode.")
            perfectTakeoverNode ! TakeoverFailedNodeZone(context.self, failedNode, neighbours(failedNode))

            process(nodeId, movies, replicaSet-failedNode, coordinates, neighbours-failedNode, transitiveNeighbourMap)
          }

          else {
            context.log.info(s"${context.self.path}\t:\t${printCoordinates(coordinates)} and ${printCoordinates(neighbours(failedNode))} " +
              s"do not perfectly match. Finding a perfectly mergeable neighbour of mine.")
            context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tAsking $perfectlyMergeableNeighbour to " +
              s"takeover my zone.")
            val newCoordinates = neighbours(failedNode)
            val newNeighbours =
              getFailedNodeNeighbours(neighbours, takeOverAckedNodes + (respondingNode -> transitiveNeighbourMap(failedNode)(respondingNode)))

            perfectlyMergeableNeighbour ! TakeoverMyZone(context.self, coordinates)

            context.log.info(s"${context.self.path}\t:\tCurrent zone coordinates - ${printCoordinates(coordinates)}, taking over $failedNode and " +
              s"changing zone coordinates to ${printCoordinates(newCoordinates)}")
            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\t new neighbours(includes overlapping/non-overlapping) - " +
              s"${printNeighboursAndCoordinates(newNeighbours - failedNode)}")

            context.log.info(s"${context.self.path}\t:\tTelling all ACKed nodes about takeover success")
            (takeOverAckedNodes + (respondingNode -> transitiveNeighbourMap(failedNode)(respondingNode)))
              .keySet
              .foreach(tn => tn ! TakeoverFinal(failedNode, context.self, newCoordinates))

            val currentlyOverlappingNeighbours =
              findNeighboursForOverlappingCoordinates(
                newNeighbours,
                newCoordinates(0)(0),
                newCoordinates(0)(1),
                newCoordinates(1)(0),
                newCoordinates(1)(1)
              )
            val currentlyNonOverlappingNeighbours = neighbours.keySet.diff(currentlyOverlappingNeighbours)
            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently overlapping neighbours - " +
              s"[${currentlyOverlappingNeighbours.mkString(", ")}]")
            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently non-overlapping neighbours - " +
              s"[${currentlyNonOverlappingNeighbours.mkString(", ")}]")

            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tRemoving all current neighbours which do not " +
              s"overlap with my new coordinates, and telling all those neighbours to remove me from their neighbour maps.")
            currentlyNonOverlappingNeighbours.foreach(n => {
              context.unwatch(n)
              n ! RemoveNeighbours(context.self)
            })

            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tTelling all neighbours which do overlap with my " +
              s"new coordinates to include me in their neighbour maps/update my coordinates.")
            currentlyOverlappingNeighbours.foreach(n => n ! NotifyNeighboursToUpdateNeighbourMaps(context.self, newCoordinates))

            ((newNeighbours -- currentlyNonOverlappingNeighbours) - failedNode).keySet.foreach(n => context.watch(n))

            process(nodeId, replicaSet(failedNode), replicaSet--currentlyNonOverlappingNeighbours-failedNode, newCoordinates,
              (newNeighbours -- currentlyNonOverlappingNeighbours) - failedNode,
              transitiveNeighbourMap - failedNode)
          }
        }

        else {
          /* Include all neighbours of failed node in my neighbour map. Tell new neighbours to put you in their neighbour maps. */
          val newCoordinates = getMergedCoordinates(coordinates, neighbours(failedNode))
          val newNeighbours =
            getFailedNodeNeighbours(neighbours, takeOverAckedNodes+(respondingNode->transitiveNeighbourMap(failedNode)(respondingNode)))
          context.log.info(s"${context.self.path}\t:\tCurrent zone coordinates - ${printCoordinates(coordinates)}, taking over $failedNode and " +
            s"changing zone coordinates to ${printCoordinates(newCoordinates)}")
          context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\t new neighbours(includes overlapping/non-overlapping) - " +
            s"${printNeighboursAndCoordinates(newNeighbours-failedNode)}")

          context.log.info(s"${context.self.path}\t:\tTelling all ACKed nodes about takeover success")
          (takeOverAckedNodes+(respondingNode->transitiveNeighbourMap(failedNode)(respondingNode)))
            .keySet
            .foreach(tn => tn ! TakeoverFinal(failedNode, context.self, newCoordinates))

          val currentlyOverlappingNeighbours =
            findNeighboursForOverlappingCoordinates(
              newNeighbours,
              newCoordinates(0)(0),
              newCoordinates(0)(1),
              newCoordinates(1)(0),
              newCoordinates(1)(1)
            )
          val currentlyNonOverlappingNeighbours = neighbours.keySet.diff(currentlyOverlappingNeighbours)
          context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently overlapping neighbours - " +
            s"[${currentlyOverlappingNeighbours.mkString(", ")}]")
          context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently non-overlapping neighbours - " +
            s"[${currentlyNonOverlappingNeighbours.mkString(", ")}]")

          context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tRemoving all current neighbours which do not " +
            s"overlap with my new coordinates, and telling all those neighbours to remove me from their neighbour maps.")
          currentlyNonOverlappingNeighbours.foreach(n => {
            context.unwatch(n)
            n ! RemoveNeighbours(context.self)
          })

          context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tTelling all neighbours which do overlap with my " +
            s"new coordinates to include me in their neighbour maps/update my coordinates.")
          currentlyOverlappingNeighbours.foreach(n => {
            context.watch(n)
            n ! NotifyNeighboursToUpdateNeighbourMaps(context.self, newCoordinates)
          })

          ((newNeighbours--currentlyNonOverlappingNeighbours)-failedNode).keySet.foreach(n => context.watch(n))

          process(nodeId, movies++replicaSet(failedNode), (replicaSet--currentlyNonOverlappingNeighbours)-failedNode, newCoordinates,
            (newNeighbours--currentlyNonOverlappingNeighbours)-failedNode, transitiveNeighbourMap-failedNode)
        }
      }
      else {
        context.log.info(s"${context.self.path}\t:\tACKs pending from " +
          s"[${printNeighboursAndCoordinates(transitiveNeighbourMap(failedNode)-respondingNode-context.self)}]")
        process(nodeId, movies, replicaSet, coordinates, neighbours, transitiveNeighbourMap+(failedNode->(transitiveNeighbourMap(failedNode)-respondingNode)), takeOverMode=takeOverMode, schedulerInstance=schedulerInstance, takeOverAckedNodes=takeOverAckedNodes+(respondingNode->transitiveNeighbourMap(failedNode)(respondingNode)))
      }

    case (context, TakeoverNack(failedNode)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tgot takeover NACK for taking over zone of $failedNode." +
        s"Cancelling my takeover attempt.")
      if (schedulerInstance != null) schedulerInstance.cancel()
      process(nodeId, movies, replicaSet, coordinates, neighbours, transitiveNeighbourMap)

    case (context, TakeoverFinal(failedNode, takeoverNode, takeoverNodeCoordinates)) =>
      context.log.info(s"${context.self.path}\t:\t$takeoverNode is taking over zone of $failedNode. Updating my neighbours to include $takeoverNode")
      context.log.info(s"${context.self.path}\t:\tnow my new neighbours are - ${printNeighboursAndCoordinates(neighbours+(takeoverNode->takeoverNodeCoordinates)-failedNode)}")
      process(nodeId, movies, replicaSet, coordinates, neighbours+(takeoverNode->takeoverNodeCoordinates)-failedNode, transitiveNeighbourMap)

    case (context, TakeoverMyZone(node, coords)) =>
      val myNewCoordinates = getMergedCoordinates(coordinates, coords)
      context.log.info(s"${context.self.path}\t:\t$node has asked me to take over its zone. " +
        s"Changing my coordinates to ${printCoordinates(myNewCoordinates)}")
      ((neighbours.keySet+node)++transitiveNeighbourMap(node).keySet-context.self).foreach(n => n ! NotifyNeighboursToUpdateNeighbourMaps(context.self, myNewCoordinates))
      process(nodeId, movies++replicaSet(node), replicaSet-node, myNewCoordinates, (neighbours+(node->coords))++transitiveNeighbourMap(node)-context.self, transitiveNeighbourMap)

    case (context, TakeoverFailedNodeZone(requesterNode, failedNode, failedNodeCoordinates)) =>
      context.log.info(s"${context.self.path}\t:\t$requesterNode has asked me to take over zone of failed node $failedNode.")
      val newCoordinates = getMergedCoordinates(coordinates, failedNodeCoordinates)
      val newNeighbours = findNeighboursForOverlappingCoordinates(
        neighbours,
        newCoordinates(0)(0),
        newCoordinates(0)(1),
        newCoordinates(1)(0),
        newCoordinates(1)(1)
      )+requesterNode

      newNeighbours.foreach(n => context.watch(n))

      val nonOverlappingNeighbours = neighbours.keySet.diff(newNeighbours)
      nonOverlappingNeighbours.foreach(n => {
        context.unwatch(n)
        n ! RemoveNeighbours(context.self)
      })

      newNeighbours.foreach(n => n ! NotifyNeighboursToUpdateNeighbourMaps(context.self, newCoordinates))
      process(nodeId, movies++replicaSet(failedNode), replicaSet--nonOverlappingNeighbours-failedNode, newCoordinates,
        neighbours--nonOverlappingNeighbours-context.self, transitiveNeighbourMap)

      /*
      * This node has a received a periodic update from one of its (existing/new) neighbours
      */
    case (context, NotifyNeighboursWithNeighbourMap(neighbourRef, neighbourCoordinates)) =>
      /*if (!neighbours.contains(neighbourRef))
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived periodic update from new neighbour $neighbourRef")
      else
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived periodic update from existing neighbour $neighbourRef")
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\t$neighbourRef sent me its neighbours")*/
      process(nodeId, movies, replicaSet, coordinates, neighbours, transitiveNeighbourMap+(neighbourRef -> neighbourCoordinates))

      /*
      * After a neighbour split/new neighbour join, this node receives an update of the neighbouring zone's coordinates
      */
    case (context, NotifyNeighboursToUpdateNeighbourMaps(neighbourRef, neighbourCoordinates)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived notification from neighbour $neighbourRef with coordinates " +
        s"${printCoordinates(neighbourCoordinates)} to update my neighbour coordinates")
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy current neighbours and coordinates are - ${printNeighboursAndCoordinates(neighbours)}")
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tNew neighbour coordinates are - ${printNeighboursAndCoordinates(neighbours+(neighbourRef->neighbourCoordinates))}")

      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tWatching $neighbourRef")
      context.watch(neighbourRef)
      process(nodeId, movies, replicaSet, coordinates, neighbours+(neighbourRef->neighbourCoordinates), transitiveNeighbourMap)

    case (context, RemoveNeighbours(neighbourRef)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tGot remove neighbour message from $neighbourRef")
      context.unwatch(neighbourRef)
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tWatching $neighbourRef")
      process(nodeId, movies, replicaSet-neighbourRef, coordinates, neighbours-neighbourRef, transitiveNeighbourMap)

    /*
    * Messages for handling storage/retrieval of movies
    */
    case m @ (context, FindNodeForStoringData(parentRef, replyTo, name, size, genre, endX, endY, requesterNode)) =>
      if (isWithinMyZone(name, coordinates, endX, endY)) {
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tStoring movie $name with me")
        replyTo ! DataStorageResponseSuccess(s"Movie $name successfully stored")

        /*
        * Store movie in cassandra table here
        */


        process(nodeId, movies+Movie(name, size, genre), replicaSet, coordinates, neighbours, transitiveNeighbourMap)
      }
      else if (neighbours.keySet.size > 1) {
        val x = getXCoordinate(name, endX)
        val y = getYCoordinate(name, endY)

        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tGot request to store movie $name. " +
          s"Hashed point ($x, $y) does not lie in my zone.")
        val nearestNeighbourToMovie = findNearestNeighbourForMovie(name, context, neighbours, endX, endY)
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tForwarding storage request of $name to " +
          s" hashed point ($x, $y) nearest neighbour $nearestNeighbourToMovie.")
        nearestNeighbourToMovie ! FindNodeForStoringData(parentRef, replyTo, name, size, genre, endX, endY, context.self)
        Behaviors.same
      }
      else {
        parentRef ! Parent.FindAnotherNodeForStorage(replyTo, name, size, genre, endX, endY)
        Behaviors.same
      }

    case m @ (context, FindSuccessorForFindingData(parentRef, replyTo, name, endX, endY, requesterNode)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tGot request for finding movie $name")
      val x = getXCoordinate(name, endX)
      val y = getYCoordinate(name, endY)
      if (isWithinMyZone(name, coordinates, endX, endY) && movies.exists(_.name == name)) {

        /* Get movie from cassandra table here */

        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tFound movie $name with me")
        replyTo ! DataResponseSuccess(movies.find(_.name == name))
      }
      else if (replicaSet.keySet.exists(replicaSet(_).exists(_.name == name))) {
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tFound replica of $name with me")
        val movieOwnerNode = replicaSet.keySet.find(replicaSet(_).exists(_.name == name)).get
        replyTo ! DataResponseSuccess(replicaSet(movieOwnerNode).find(_.name == name))
      }
      else if (isWithinMyZone(name, coordinates, endX, endY)) {
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMovie $name was not stored in me. 404!")
        replyTo ! DataResponseFailed(s"Requested movie $name could not be found. 404!")
      }
      else if (neighbours.keySet.size > 1) {
        val nearestNeighbour = findNearestNeighbourForMovie(name, context, neighbours, endX, endY)
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tCould not find movie $name with me. " +
          s"Movie hashes to point ($x, $y), which is not in my zone. Forwarding search to nearest neighbour $nearestNeighbour")
        nearestNeighbour ! FindSuccessorForFindingData(parentRef, replyTo, name, endX, endY, context.self)
      }
      else
        parentRef ! FindAnotherNodeForQuery(replyTo, name, endX, endY)

      Behaviors.same
  }
    .receiveSignal {
      case (context, Terminated(failedNode)) =>
        val failedNodeRef = failedNode.unsafeUpcast[Command]

        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\t$failedNodeRef has failed.")

        val myArea = findMyArea(coordinates)
        val randomTime = Random.nextInt(10)
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy zone area is $myArea")

        if (transitiveNeighbourMap.contains(failedNodeRef) && transitiveNeighbourMap(failedNodeRef).size == 1) {
          /* This node is the only neighbour of the failed node */
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tI am the only neighbour of $failedNodeRef. Taking over its zone.")

          val isMergeable: Boolean = isPerfectMerge(coordinates, neighbours(failedNodeRef))

          if (!isMergeable) {
            val perfectlyMergeableNeighbour = findPerfectMergeableNeighbour(coordinates, neighbours)
            context.log.info(s"${context.self.path}\t:\t${printCoordinates(coordinates)} and ${printCoordinates(neighbours(failedNodeRef))} " +
              s"do not perfectly match. Finding a perfectly mergeable neighbour of mine.")

            if (perfectlyMergeableNeighbour == null) {
              context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tI do not have a perfectly mergeable neighbour. " +
                s"Finding one from neighbours of $failedNodeRef.")
              val perfectTakeoverNode = findPerfectMergeableNeighbour(neighbours(failedNodeRef), transitiveNeighbourMap(failedNodeRef))

              context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tAsking $perfectTakeoverNode to takeover zone of $failedNodeRef.")
              perfectTakeoverNode ! TakeoverFailedNodeZone(context.self, failedNodeRef, neighbours(failedNodeRef))

              process(nodeId, movies, replicaSet-failedNodeRef, coordinates, neighbours-failedNodeRef-context.self, transitiveNeighbourMap)
            }

            else {
              context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tAsking $perfectlyMergeableNeighbour to " +
                s"takeover my zone.")
              perfectlyMergeableNeighbour ! TakeoverMyZone(context.self, coordinates)

              val newCoordinates = neighbours(failedNodeRef)
              context.log.info(s"${context.self.path}\t:\tCurrent zone coordinates - ${printCoordinates(coordinates)}, taking over " +
                s"$failedNodeRef and changing zone coordinates to ${printCoordinates(newCoordinates)}")

              val currentlyOverlappingNeighbours =
                findNeighboursForOverlappingCoordinates(
                  neighbours,
                  newCoordinates(0)(0),
                  newCoordinates(0)(1),
                  newCoordinates(1)(0),
                  newCoordinates(1)(1)
                )
              val currentlyNonOverlappingNeighbours = neighbours.keySet.diff(currentlyOverlappingNeighbours)
              context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently overlapping neighbours - " +
                s"[${currentlyOverlappingNeighbours.mkString(", ")}]")
              context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently non-overlapping neighbours - " +
                s"[${currentlyNonOverlappingNeighbours.mkString(", ")}]")

              context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tRemoving all current neighbours which do not " +
                s"overlap with my new coordinates, and telling all those neighbours to remove me from their neighbour maps.")
              currentlyNonOverlappingNeighbours.foreach(n => {
                context.unwatch(n)
                n ! RemoveNeighbours(context.self)
              })

              context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tTelling all current neighbours which do overlap" +
                s" with my new coordinates to update their neighbour maps with my new coordinates")
              currentlyOverlappingNeighbours.foreach(n => n ! NotifyNeighboursToUpdateNeighbourMaps(context.self, newCoordinates))

              ((neighbours -- currentlyNonOverlappingNeighbours) - failedNodeRef).keySet.foreach(n => context.watch(n))

              process(nodeId, replicaSet(failedNodeRef), replicaSet--currentlyNonOverlappingNeighbours-failedNodeRef, newCoordinates,
                (neighbours -- currentlyNonOverlappingNeighbours)-failedNodeRef-context.self, transitiveNeighbourMap - failedNodeRef)
            }
          }

          else {
            val newCoordinates = getMergedCoordinates(coordinates, neighbours(failedNodeRef))

            context.log.info(s"${context.self.path}\t:\tCurrent zone coordinates - ${printCoordinates(coordinates)}, taking over $failedNodeRef and " +
              s"changing zone coordinates to ${printCoordinates(newCoordinates)}")

            val currentlyOverlappingNeighbours =
              findNeighboursForOverlappingCoordinates(
                neighbours,
                newCoordinates(0)(0),
                newCoordinates(0)(1),
                newCoordinates(1)(0),
                newCoordinates(1)(1)
              )
            val currentlyNonOverlappingNeighbours = neighbours.keySet.diff(currentlyOverlappingNeighbours)
            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently overlapping neighbours - " +
              s"[${currentlyOverlappingNeighbours.mkString(", ")}]")
            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tCurrently non-overlapping neighbours - " +
              s"[${currentlyNonOverlappingNeighbours.mkString(", ")}]")

            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tRemoving all current neighbours which do not " +
              s"overlap with my new coordinates, and telling all those neighbours to remove me from their neighbour maps.")
            currentlyNonOverlappingNeighbours.foreach(n => {
              context.unwatch(n)
              n ! RemoveNeighbours(context.self)
            })

            context.log.info(s"${context.self.path} - ${printCoordinates(newCoordinates)}\t:\tTelling all current neighbours which do overlap" +
              s" with my new coordinates to update their neighbour maps with my new coordinates")
            currentlyOverlappingNeighbours.foreach(n => n ! NotifyNeighboursToUpdateNeighbourMaps(context.self, newCoordinates))

            ((neighbours--currentlyNonOverlappingNeighbours)-failedNodeRef).keySet.foreach(n => context.watch(n))

            process(nodeId, movies++replicaSet(failedNodeRef), replicaSet--currentlyNonOverlappingNeighbours-failedNodeRef, newCoordinates,
              (neighbours--currentlyNonOverlappingNeighbours)-failedNodeRef-context.self, transitiveNeighbourMap-failedNodeRef)
          }
        }

        else if (transitiveNeighbourMap.contains(failedNodeRef)) {
          /* The failed node has other neighbours. Initiate takeover mechanism */
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tWaiting for ${(myArea+randomTime)/100} milliseconds before initiating takeover.")

          implicit val executionContextExecutor: ExecutionContextExecutor = context.executionContext
          val schInstance = context.system.scheduler.scheduleOnce(((myArea+randomTime)/100).milliseconds, () => {
            transitiveNeighbourMap(failedNodeRef).keySet.foreach(tn => {
              if (tn != context.self) tn ! Takeover(failedNodeRef, myArea, nodeId, context.self)
            })
          })
          process(nodeId, movies, replicaSet, coordinates, neighbours-context.self, transitiveNeighbourMap, takeOverMode=true, schedulerInstance=schInstance)
        }
        else {
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tNode $failedNodeRef hasn't sent me its neighbour details." +
            s" Not taking over its zone.")
          process(nodeId, movies, replicaSet, coordinates, neighbours-context.self, transitiveNeighbourMap)
        }
    }

  /**
   * Given the neighbours coordinates and a pair of start-end coordinates, this function will return the set of all existing neighbours
   * which overlap with those start-end coordinates
   * @param neighbours Existing neighbour coordinates
   * @param startX left x coordinate
   * @param endX right x coordinate
   * @param startY bottom y coordinate
   * @param endY top y coordinate
   */
  def findNeighboursForOverlappingCoordinates(neighbours: Map[ActorRef[Command], Array[Array[Double]]], startX: Double, endX: Double,
                                              startY: Double, endY: Double): Set[ActorRef[Command]] = {
    val newNeighbourSet = mutable.Set[ActorRef[Command]]()

    neighbours.keySet.foreach(k => {
      if ((overlaps(startX, endX, neighbours(k)(0)(0), neighbours(k)(0)(1)) && (startY == neighbours(k)(1)(1) || endY == neighbours(k)(1)(0)))
        ||
        (overlaps(startY, endY, neighbours(k)(1)(0), neighbours(k)(1)(1)) && (startX == neighbours(k)(0)(1) || endX == neighbours(k)(0)(0)))
      )
        newNeighbourSet += k
    })
    newNeighbourSet.toSet
  }

  /**
   * Given 2 pairs of x or 2 pairs of y coordinates, this function determines whether those pairs overlap or not
   * @param p1 start x or y coordinate of 1st pair
   * @param p2 end x or y coordinate of 1st pair
   * @param p3 start x or y coordinate of 2nd pair
   * @param p4 end x or y coordinate of 2nd pair
   */
  def overlaps(p1: Double, p2: Double, p3: Double, p4: Double): Boolean =
    (p1 <= p3 && p3 < p2) || (p3 <= p1 && p1 < p4) || (p3 <= p1 && p2 <= p4) || (p1 <= p3 && p4 <= p2)

  /**
   * Given a set of neighbours and a map of neighbour refs to coordinates, this function will returns a new map of
   * neighbours from the set and corresponding coordinates from the input map
   * @param neighbourSet Set of neighbours whose coordinates is to be found
   * @param neighboursMap Map of existing neighbours to their coordinates
   */
  def getNeighbourCoordinatesFromNewNeighbourSet(neighbourSet: Set[ActorRef[Command]],
                                                 neighboursMap: Map[ActorRef[Command], Array[Array[Double]]]):
  Map[ActorRef[Command], Array[Array[Double]]] = {

    @tailrec
    def getNeighboursCoordinates(neighbourSet: Set[ActorRef[Command]], neighboursMap: Map[ActorRef[Command], Array[Array[Double]]],
                                 resultMap: Map[ActorRef[Command], Array[Array[Double]]]): Map[ActorRef[Command], Array[Array[Double]]] = {
      if (neighbourSet.size == 1) resultMap+(neighbourSet.head -> neighboursMap(neighbourSet.head))
      else getNeighboursCoordinates(neighbourSet-neighbourSet.head, neighboursMap, resultMap+(neighbourSet.head -> neighboursMap(neighbourSet.head)))
    }

    if (neighbourSet.isEmpty) Map.empty
    else getNeighboursCoordinates(neighbourSet, neighboursMap, Map.empty)
  }

  /**
   * This function will split this node's coordinates along the specified axis and returns the split coordinates
   * @param splitAxis Axis along which coordinates are to be split
   * @param coordinates Existing coordinates
   */
  def splitMyCoordinates(splitAxis: Character, coordinates: Array[Array[Double]]): Array[Array[Double]] = {
    val startX = coordinates(0)(0)
    val endX = coordinates(0)(1)
    val startY = coordinates(1)(0)
    val endY = coordinates(1)(1)
    if (splitAxis == 'X') Array(Array(startX, (startX+endX)/2D), Array(startY, endY))
    else Array(Array(startX, endX), Array(startY, (startY+endY)/2D))
  }

  def printCoordinates(coordinates: Array[Array[Double]]): String = {
    if (coordinates.isEmpty || coordinates(0).isEmpty) ""
    else s"[(${coordinates(0).mkString(" ")}), (${coordinates(1).mkString(" ")})]"
  }

  def printNeighboursAndCoordinates(neighbourMap: Map[ActorRef[Command], Array[Array[Double]]]): String = {
    val result = new StringBuffer()
    result.append("\n")
    neighbourMap.keySet.foreach(k => result.append(k).append(" -> ").append(printCoordinates(neighbourMap(k))).append("\n"))
    result.toString
  }

  def findMyArea(coordinates: Array[Array[Double]]): Double = (coordinates(0)(1)-coordinates(0)(0)) * (coordinates(1)(1)-coordinates(1)(0))

  def takeoverCoordinates(coordinates: Array[Array[Double]], failedNodeCoordinates: Array[Array[Double]]): Array[Array[Double]] = {
    val takeoverNodeStartX = coordinates(0)(0)
    val takeoverNodeEndX = coordinates(0)(1)
    val takeoverNodeStartY = coordinates(1)(0)
    val takeoverNodeEndY = coordinates(1)(1)

    val failedNodeStartX = failedNodeCoordinates(0)(0)
    val failedNodeEndX = failedNodeCoordinates(0)(1)
    val failedNodeStartY = failedNodeCoordinates(1)(0)
    val failedNodeEndY = failedNodeCoordinates(1)(1)

    Array(
      Array(java.lang.Double.min(takeoverNodeStartX, failedNodeStartX), java.lang.Double.max(takeoverNodeEndX, failedNodeEndX)),
      Array(java.lang.Double.min(takeoverNodeStartY, failedNodeStartY), java.lang.Double.max(takeoverNodeEndY, failedNodeEndY))
    )
  }

  @tailrec
  def getFailedNodeNeighbours(myNeighbours: Map[ActorRef[Command], Array[Array[Double]]],
                              failedNodeNeighbours: Map[ActorRef[Command], Array[Array[Double]]]):
  Map[ActorRef[Command], Array[Array[Double]]] = {
    if (failedNodeNeighbours.isEmpty) myNeighbours
    else getFailedNodeNeighbours(myNeighbours+(failedNodeNeighbours.keySet.head -> failedNodeNeighbours(failedNodeNeighbours.keySet.head)),
      failedNodeNeighbours-failedNodeNeighbours.keySet.head)
  }

  /**
   * Merges both coordinate end points into a rectangle and returns the resulting end points
   * @param coordinateSet1 first set of coordinates
   * @param coordinateSet2 second set of coordinates
   */
  def getMergedCoordinates(coordinateSet1: Array[Array[Double]], coordinateSet2: Array[Array[Double]]): Array[Array[Double]] = {
    val sx1 = coordinateSet1(0)(0)
    val ex1 = coordinateSet1(0)(1)
    val sy1 = coordinateSet1(1)(0)
    val ey1 = coordinateSet1(1)(1)

    val sx2 = coordinateSet2(0)(0)
    val ex2 = coordinateSet2(0)(1)
    val sy2 = coordinateSet2(1)(0)
    val ey2 = coordinateSet2(1)(1)

    Array(
      Array(java.lang.Double.min(sx1, sx2), java.lang.Double.max(ex1, ex2)),
      Array(java.lang.Double.min(sy1, sy2), java.lang.Double.max(ey1, ey2))
    )
  }

  /**
   * Checks if given end points form a perfect rectangle or not after merging
   * @param coordinateSet1 first set of coordinates
   * @param coordinateSet2 second set of coordinates
   */
  def isPerfectMerge(coordinateSet1: Array[Array[Double]], coordinateSet2: Array[Array[Double]]): Boolean = {
    val sx1 = coordinateSet1(0)(0)
    val ex1 = coordinateSet1(0)(1)
    val sy1 = coordinateSet1(1)(0)
    val ey1 = coordinateSet1(1)(1)

    val sx2 = coordinateSet2(0)(0)
    val ex2 = coordinateSet2(0)(1)
    val sy2 = coordinateSet2(1)(0)
    val ey2 = coordinateSet2(1)(1)

    (sx1 == sx2 && ex1 == ex2) || (sy1 == sy2 && ey1 == ey2)
  }

  /**
   * Finds a neighbour (if it exists) for which there is at least 1 pair of perfectly overlapping end points (x or y)
   * @param coordinates End points of this node's zone
   * @param neighbours This node's neighbours
   */
  def findPerfectMergeableNeighbour(coordinates: Array[Array[Double]], neighbours: Map[ActorRef[Command], Array[Array[Double]]]):
  ActorRef[Command] = {
    var perfectMergeableNeighbour: ActorRef[Command] = null
    neighbours.keySet.foreach(n => if (isPerfectMerge(coordinates, neighbours(n))) perfectMergeableNeighbour = n)
    perfectMergeableNeighbour
  }

  /**
   * Checks if the given movie is stored with this node or not. It hashes the movie name using MD5 2 times to get the
   * point's x and y coordinates and checks if the point lies in this node's zone or not.
   * @param movieName Name of the movie
   * @param coordinates End points of this node's zone
   */
  def isWithinMyZone(movieName: String, coordinates: Array[Array[Double]], endX: Int, endY: Int): Boolean = {
    val x = getXCoordinate(movieName, endX)
    val y = getYCoordinate(movieName, endY)
    (coordinates(0)(0) <= x && x <= coordinates(0)(1)) && (coordinates(1)(0) <= y && y <= coordinates(1)(1))
  }

  /**
   * Given the movie's hashed point coordinates and neighbours, this function returns the neighbour whose zone midpoint is
   * nearest to the movie's hashed point.
   * @param movieName Name of the movie
   * @param context Execution context of this actor
   * @param neighbours Map of neighbour nodes to their zone coordinates
   */
  def findNearestNeighbourForMovie(movieName: String, context: ActorContext[Command],
                                   neighbours: Map[ActorRef[Command], Array[Array[Double]]],
                                   endX: Int, endY: Int): ActorRef[Command] = {
    val x = getXCoordinate(movieName, endX)
    val y = getYCoordinate(movieName, endY)
    var nearestNeighbour: ActorRef[Command] = null
    var minDistance = Double.MaxValue
    context.log.info(s"${context.self.path}\t:\tfinding nearest among ${printNeighboursAndCoordinates(neighbours)}")

    neighbours.foreach(e => {
      val startXPointOfNeighbour = e._2(0)(0)
      val startYPointOfNeighbour = e._2(1)(0)
      val endXPointOfNeighbour = e._2(0)(1)
      val endYPointOfNeighbour = e._2(1)(1)

      val randomXPointOfNeighbour = startXPointOfNeighbour + Random.nextInt(endXPointOfNeighbour.toInt-startXPointOfNeighbour.toInt+1)
      val randomYPointOfNeighbour = startYPointOfNeighbour + Random.nextInt(endYPointOfNeighbour.toInt-startYPointOfNeighbour.toInt+1)

      val distance = getDistance(x, y, randomXPointOfNeighbour, randomYPointOfNeighbour)
      context.log.info(s"${context.self.path} - Distance calculated to ${e._1} is $distance")
      if (distance <= minDistance) {
        minDistance = distance
        nearestNeighbour = e._1
      }
    })
    nearestNeighbour
  }

  def getXCoordinate(movieName: String, endX: Int): Int =
    getSignedHash(endX, movieName)+1

  def getYCoordinate(movieName: String, endY: Int): Int =
    getSignedHash(endY, movieName)+1

  /**
   * Compute straight line distance between movie's hashed point and middle point of a neighbouring node's zone.
   * It uses the euclidean distance for computation.
   * @param x1 x coordinate of 1st point
   * @param y1 y coordinate of 1st point
   * @param x2 x coordinate of 2nd point
   * @param y2 y coordinate of 2nd point
   */
  def getDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = Math.sqrt(Math.pow(x2-x1, 2)+Math.pow(y2-y1, 2))

  def getState(coordinates: Array[Array[Double]], movies: Set[Movie], replicaSet: Map[ActorRef[Command], Set[Movie]]): State = {
    val coordinateString = s"(startX=${coordinates(0)(0)}, endX=${coordinates(0)(1)}), (startY=${coordinates(1)(0)}, endY=${coordinates(1)(1)})"
    State(coordinateString, movies, replicaSet)
  }
}
