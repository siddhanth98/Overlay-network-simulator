package com.can

import akka.actor.Cancellable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{DurationDouble, DurationInt}
import scala.util.Random

/**
 * This actor represents a node in the CAN
 */
object Node {
  trait Command

  /**
   * Message sent by a newly joined node to a random node already in the CAN
   */
  final case class Join(replyTo: ActorRef[Command], splitAxis: Character) extends Command

  /**
   * Set of messages for periodically notifying neighbours with current neighbour set
   */
  final case object NotifyMyNeighbours extends Command
  final case class NotifyNeighboursWithNeighbourMap(neighbourRef: ActorRef[Command],
                                                    neighbours: Set[ActorRef[Command]]) extends Command
  final case object CheckNeighbourStatus extends Command

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

  /**
   * Set of messages for the takeover mechanism
   */
  final case class Takeover(failedNode: ActorRef[Command], area: Double, requestingNodeId: Int, requestingNodeRef: ActorRef[Command])
    extends Command
  final case class TakeoverAck(failedNode: ActorRef[Command], node: ActorRef[Command]) extends Command
  final case class TakeoverNack(failedNode: ActorRef[Command]) extends Command



  /**
   * Constructor for 1st node to join the CAN
   */
  def apply(nodeId: Int): Behavior[Command] = Behaviors.setup{context =>
    context.log.info(s"${context.self.path}\t:\tI am the first node in the CAN")
    Behaviors.withTimers{timer =>
      timer.startTimerAtFixedRate(NotifyMyNeighbours, 100.milliseconds)
      timer.startTimerAtFixedRate(CheckNeighbourStatus, 120.milliseconds)
      process(nodeId, Set.empty, Array(Array(1, 100), Array(1, 100)), Map.empty, Map.empty, Map.empty)
    }
  }

  /**
   * Constructor for successive node to join the CAN
   */
  def apply(nodeId: Int, existingNodeRef: ActorRef[Command], splitAxis: Character): Behavior[Command] = Behaviors.setup{context =>
    context.log.info(s"${context.self.path}\t:\tI am not the first node to join the CAN. Got existing node $existingNodeRef")
    context.log.info(s"${context.self.path}\t:\tSending join message to $existingNodeRef")
    Behaviors.withTimers{timer =>
      timer.startTimerAtFixedRate(NotifyMyNeighbours, 100.milliseconds)
      timer.startTimerAtFixedRate(CheckNeighbourStatus, 120.milliseconds)
      existingNodeRef ! Join(context.self, splitAxis)
      process(nodeId, Set.empty, Array[Array[Double]](), Map.empty, Map.empty, Map.empty)
    }
  }

  /**
   * Defines the behavior of the node actor
   */
  def process(nodeId: Int, movies: Set[Movie], coordinates: Array[Array[Double]], neighbours: Map[ActorRef[Command], Array[Array[Double]]],
              transitiveNeighbourMap: Map[ActorRef[Command], Set[ActorRef[Command]]], neighbourStatus: Map[ActorRef[Command], Boolean],
              takeOverMode: Boolean=false, schedulerInstance: Cancellable=null):
  Behavior[Command] = Behaviors.receive {
        /*
         * Notifies neighbours about this node's current neighbours
         */
    case (context, NotifyMyNeighbours) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tNotifying neighbours...")
      neighbours.keySet.foreach(k => k ! NotifyNeighboursWithNeighbourMap(context.self, neighbours.keySet.filterNot(o => o != k)))
      Behaviors.same

      /*
      * New node gets its new coordinates and neighbour coordinates from split node
      */
    case (context, SplitResponse(newCoordinates, neighboursMap)) =>
      context.log.info(s"${context.self.path}\t:\tReceived split response message with new coordinates - [${printCoordinates(newCoordinates)}] " +
        s"and new neighbours refs - ${printNeighboursAndCoordinates(neighboursMap)}")
      context.log.info(s"${context.self.path}\t:\tNotifying neighbours to update their neighbour maps...")
      neighboursMap.keySet.foreach(k => k ! NotifyNeighboursToUpdateNeighbourMaps(context.self, newCoordinates))
      process(nodeId, movies, newCoordinates, neighboursMap, transitiveNeighbourMap, neighbourStatus)

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
        val newNodeCoordinates = Array(Array((startX + endX - 1)/2D, endX), Array(startY, endY))
        val newNodeNeighbourSet = findNeighboursForOverlappingCoordinates(neighbours, (startX + endX - 1)/2D, endX, startY, endY)
        val newNodeNeighbourMap = getNeighbourCoordinatesFromNewNeighbourSet(newNodeNeighbourSet, neighbours)

        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSplitting myself and giving coordinates [${printCoordinates(newNodeCoordinates)}] to" +
          s"$replyTo, along with the following neighbours and their respective coordinates - ${printNeighboursAndCoordinates(newNodeNeighbourMap)}, including myself")
        val mySplitCoordinates = splitMyCoordinates(splitAxis, coordinates)

        replyTo ! SplitResponse(newNodeCoordinates, newNodeNeighbourMap + (context.self -> mySplitCoordinates))

        /* Update existing node's neighbours after split and notify current neighbours */

        val myUpdatedNeighbours = getNeighbourCoordinatesFromNewNeighbourSet(
          findNeighboursForOverlappingCoordinates(neighbours, startX, (startX+endX-1)/2D, startY, endY), neighbours
        )
        neighbours.keySet.diff(myUpdatedNeighbours.keySet).foreach(o => o ! RemoveNeighbours(context.self))
        myUpdatedNeighbours.keySet.foreach(k => k ! NotifyNeighboursToUpdateNeighbourMaps(context.self, mySplitCoordinates))
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy split coordinates are now - [${printCoordinates(mySplitCoordinates)}] and new neighbours" +
          s"are - ${printNeighboursAndCoordinates(myUpdatedNeighbours)}")
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tPutting $replyTo as my new neighbour")
        process(nodeId, movies, mySplitCoordinates, myUpdatedNeighbours+(replyTo -> newNodeCoordinates), transitiveNeighbourMap, neighbourStatus)
      }

      else {
        /* Split along the y axis horizontally, send new node its new upper zone coordinates and neighbours */
        val newNodeCoordinates = Array(Array(startX, endX), Array((startY+endY-1)/2D, endY))
        val newNodeNeighbourSet = findNeighboursForOverlappingCoordinates(neighbours, startX, endX, (startY+endY-1)/2D, endY)
        val newNodeNeighbourMap = getNeighbourCoordinatesFromNewNeighbourSet(newNodeNeighbourSet, neighbours)

        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSplitting myself and giving coordinates [${printCoordinates(newNodeCoordinates)}] to" +
          s"$replyTo, along with the following neighbours and their respective coordinates - ${printNeighboursAndCoordinates(newNodeNeighbourMap)}, including myself")

        val mySplitCoordinates = splitMyCoordinates(splitAxis, coordinates)
        replyTo ! SplitResponse(newNodeCoordinates, newNodeNeighbourMap+(context.self -> mySplitCoordinates))

        /* Update existing node's neighbours after split */
        val myUpdatedNeighbours = getNeighbourCoordinatesFromNewNeighbourSet(
          findNeighboursForOverlappingCoordinates(neighbours, startX, endX, startY, (startY+endY-1)/2D), neighbours
        )
        neighbours.keySet.diff(myUpdatedNeighbours.keySet).foreach(o => o ! RemoveNeighbours(context.self))
        myUpdatedNeighbours.keySet.foreach(k => k ! NotifyNeighboursToUpdateNeighbourMaps(context.self, mySplitCoordinates))
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy split coordinates are now - [${printCoordinates(mySplitCoordinates)}] and new neighbours" +
          s" are - ${printNeighboursAndCoordinates(myUpdatedNeighbours)}")
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tPutting $replyTo as my new neighbour")
        process(nodeId, movies, mySplitCoordinates, myUpdatedNeighbours+(replyTo -> newNodeCoordinates), transitiveNeighbourMap, neighbourStatus)
      }

    case (context, CheckNeighbourStatus) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tChecking neighbour statuses...")
      val failedNeighbour = neighbourStatus.find(_._2 == false).map(_._1).orNull
      if (failedNeighbour != null) context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tNode $failedNeighbour has failed")

      val myArea = findMyArea(coordinates)
      val randomTime = Random.nextInt(10)
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy zone area is $myArea")
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tWaiting for ${myArea+randomTime} milliseconds before initiating my takeover attempt")
      implicit val executionContext: ExecutionContextExecutor = context.system.executionContext

      val schInstance = context.system.scheduler.scheduleOnce((myArea+randomTime).milliseconds, () => {
        transitiveNeighbourMap(failedNeighbour).foreach(n => n ! Takeover(failedNeighbour, myArea, nodeId, context.self))
      })

      val newNeighbourStatus = resetNeighbourStatus(neighbours.keySet, Map.empty)
      process(nodeId, movies, coordinates, neighbours, transitiveNeighbourMap, newNeighbourStatus, takeOverMode=true,
        schedulerInstance=schInstance)

    case (context, Takeover(failedNode, area, requestingNodeId, requestingNodeRef)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tGot request for takeover from transitive neighbour $requestingNodeRef")

      if (!takeOverMode) {
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tcancelling my takeover attempt")
        schedulerInstance.cancel()
        process(nodeId, movies, coordinates, neighbours, transitiveNeighbourMap, neighbourStatus)
      }
      else {
        val myArea = findMyArea(coordinates)
        if (area < myArea || (requestingNodeId < nodeId && area == myArea)) {
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tcancelling my takeover attempt")
          schedulerInstance.cancel()
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tSending takeover ACK to $requestingNodeRef")
          requestingNodeRef ! TakeoverAck(failedNode, context.self)
        }
        else {
          context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tsending takeover NACK to $requestingNodeRef")
          requestingNodeRef ! TakeoverNack(failedNode)
        }
        process(nodeId, movies, coordinates, neighbours, transitiveNeighbourMap, neighbourStatus, takeOverMode, schedulerInstance)
      }

    case (context, TakeoverAck(failedNode, respondingNode)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\treceived takeover ACK from $respondingNode")
      if (transitiveNeighbourMap(failedNode).contains(respondingNode) && transitiveNeighbourMap(failedNode).size == 1) {
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tI have received all ACK for taking over " +
          s"zone of $failedNode.")
        process(nodeId, movies, takeoverCoordinates(coordinates, neighbours(failedNode)), neighbours, transitiveNeighbourMap, neighbourStatus)
      }
      else
        process(nodeId, movies, coordinates, neighbours,
          transitiveNeighbourMap+(failedNode->(transitiveNeighbourMap(failedNode)-respondingNode)), neighbourStatus)

    case (context, TakeoverNack(failedNode)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tgot takeover NACK for taking over zone of $failedNode." +
        s"Cancelling my takeover attempt.")
      schedulerInstance.cancel()
      process(nodeId, movies, coordinates, neighbours, transitiveNeighbourMap, neighbourStatus)

      /*
      * This node has a received a periodic update from one of its (existing/new) neighbours
      */
    case (context, NotifyNeighboursWithNeighbourMap(neighbourRef, neighbourSet)) =>
      if (!neighbours.contains(neighbourRef))
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived periodic update from new neighbour $neighbourRef")
      else
        context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived periodic update from existing neighbour $neighbourRef")

      process(nodeId, movies, coordinates, neighbours, transitiveNeighbourMap+(neighbourRef -> neighbourSet), neighbourStatus+(neighbourRef->true))

      /*
      * After a neighbour split/new neighbour join, this node receives an update of the neighbouring zone's coordinates
      */
    case (context, NotifyNeighboursToUpdateNeighbourMaps(neighbourRef, neighbourCoordinates)) =>
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tReceived notification from neighbour $neighbourRef with coordinates " +
        s"${printCoordinates(neighbourCoordinates)} to update my neighbour coordinates")
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tMy current neighbours and coordinates are - ${printNeighboursAndCoordinates(neighbours)}")
      context.log.info(s"${context.self.path} - ${printCoordinates(coordinates)}\t:\tNew neighbour coordinates are - ${printNeighboursAndCoordinates(neighbours+(neighbourRef->neighbourCoordinates))}")
      process(nodeId, movies, coordinates, neighbours+(neighbourRef->neighbourCoordinates), transitiveNeighbourMap, neighbourStatus)

    case (context, RemoveNeighbours(neighbourRef)) =>
      context.log.info(s"${context.self.path}\t:\tGot remove neighbour message from $neighbourRef")
      process(nodeId, movies, coordinates, neighbours-neighbourRef, transitiveNeighbourMap, neighbourStatus)
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
    if (splitAxis == 'X') Array(Array(startX, (startX+endX-1)/2D), Array(startY, endY))
    else Array(Array(startX, endX), Array(startY, (startY+endY-1)/2D))
  }

  /**
   * This function will update neighbour coordinates of this node, after a neighbouring node either splits or joins the CAN
   * based on whether the neighbour is currently between this node and any of this node's existing neighbours
   * @param neighbourRef Neighbour which just split or joined
   * @param neighbours Existing neighbour coordinates to be updated
   * @param startX left x coordinate of neighbour
   * @param endX right x coordinate of neighbour
   * @param startY bottom y coordinate of neighbour
   * @param endY top y coordinate of neighbour
   */
  def updateMyNeighbourCoordinates(neighbourRef: ActorRef[Command], neighbours: Map[ActorRef[Command], Array[Array[Double]]],
                                   startX: Double, endX: Double, startY: Double, endY: Double,
                                   myCoordinates: Array[Array[Double]]): Map[ActorRef[Command], Array[Array[Double]]] = {

    @tailrec
    def updateNeighbourCoordinates(neighbourRef: ActorRef[Command], neighbours: Map[ActorRef[Command], Array[Array[Double]]],
                                   startX: Double, endX: Double, startY: Double, endY: Double, myCoordinates: Array[Array[Double]],
                                   resultMap: Map[ActorRef[Command], Array[Array[Double]]]): Map[ActorRef[Command], Array[Array[Double]]] = {
      val existingNeighbourStartX = neighbours(neighbours.keySet.head)(0)(0)
      val existingNeighbourEndX = neighbours(neighbours.keySet.head)(0)(1)
      val existingNeighbourStartY = neighbours(neighbours.keySet.head)(1)(0)
      val existingNeighbourEndY = neighbours(neighbours.keySet.head)(1)(1)

      val myStartX = myCoordinates(0)(0)
      val myEndX = myCoordinates(0)(1)
      val myStartY = myCoordinates(1)(0)
      val myEndY = myCoordinates(1)(1)

      if (neighbours.keySet.size == 1) {
        if (
              isInBetween(existingNeighbourStartX, existingNeighbourEndX, startX, endX, myStartX, myEndX) ||
              isInBetween(myStartX, myStartY, existingNeighbourStartX, existingNeighbourEndX, startX, endX) ||
              isInBetween(existingNeighbourStartY, existingNeighbourEndY, myStartY, myEndY, startY, endY) ||
              isInBetween(myStartY, myEndY, startY, endY, existingNeighbourStartY, existingNeighbourEndY)
        )
          resultMap + (neighbourRef -> Array(Array(startX, endX), Array(startY, endY)))
        else
          resultMap +
            (neighbours.keySet.head ->
              Array(
                Array(neighbours(neighbours.keySet.head)(0)(0), neighbours(neighbours.keySet.head)(0)(1)),
                Array(neighbours(neighbours.keySet.head)(1)(0), neighbours(neighbours.keySet.head)(1)(1))
              ))
      }
      else {
        if (
            isInBetween(existingNeighbourStartX, existingNeighbourEndX, startX, endX, myStartX, myEndX) ||
            isInBetween(myStartX, myStartY, existingNeighbourStartX, existingNeighbourEndX, startX, endX) ||
            isInBetween(existingNeighbourStartY, existingNeighbourEndY, myStartY, myEndY, startY, endY) ||
            isInBetween(myStartY, myEndY, startY, endY, existingNeighbourStartY, existingNeighbourEndY)
        )
          updateNeighbourCoordinates(
          neighbourRef, neighbours-neighbours.keySet.head, startX, endX, startY, endY, myCoordinates,
            resultMap + (neighbourRef -> Array(Array(startX, endX), Array(startY, endY)))
          )
        else
          updateNeighbourCoordinates(
            neighbourRef, neighbours-neighbours.keySet.head, startX, endX, startY, endY, myCoordinates,
            resultMap +
              (neighbours.keySet.head ->
                Array(
                  Array(neighbours(neighbours.keySet.head)(0)(0), neighbours(neighbours.keySet.head)(0)(1)),
                  Array(neighbours(neighbours.keySet.head)(1)(0), neighbours(neighbours.keySet.head)(1)(1))
                ))
          )
      }
    }

    def isInBetween(p1: Double, p2: Double, p3: Double, p4: Double, p5: Double, p6: Double): Boolean = p2 <= p3 && p4 <= p5

    updateNeighbourCoordinates(neighbourRef, neighbours, startX, endX, startY, endY, myCoordinates, Map.empty)
  }

  @tailrec
  def resetNeighbourStatus(neighbours: Set[ActorRef[Command]], resultMap: Map[ActorRef[Command], Boolean]): Map[ActorRef[Command], Boolean] = {
    if (neighbours.isEmpty) resultMap
    else resetNeighbourStatus(neighbours-neighbours.head, resultMap+(neighbours.head->false))
  }

  def printCoordinates(coordinates: Array[Array[Double]]): String = {
    if (coordinates(0).isEmpty) ""
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
}
