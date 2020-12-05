package com.can

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

/**
 * This actor represents a node in the CAN
 */
object Node {
  trait Command

  /**
   * Message sent by a newly joined node to a random node already in the CAN
   */
  final case class Join(replyTo: ActorRef[Command], splitAxis: Int) extends Command

  /**
   * Set of messages for periodically notifying neighbours with current neighbour set
   */
  final case object NotifyMyNeighbours extends Command
  final case class NotifyNeighboursWithNeighbourMap(neighbourRef: ActorRef[Command],
                                                    neighbours: Map[ActorRef[Command], Array[Array[Int]]]) extends Command

  /**
   * Message received by new node as part of the split join process
   */
  final case class SplitResponse(newCoordinates: Array[Array[Int]], neighbours: Map[ActorRef[Command], Array[Array[Int]]]) extends Command

  /**
   * Message sent by new node to its neighbours to update their neighbour maps
   */
  final case class NotifyNeighboursToUpdateNeighbourMaps(newNeighbourRef: ActorRef[Command], newCoordinates: Array[Array[Int]]) extends Command

  /**
   * Message sent by existing split node to its existing neighbours to update their neighbour maps
   */
  final case class UpdateMyNeighbourCoordinates(neighbourRef: ActorRef[Command], coordinates: Array[Array[Int]]) extends Command

  /**
   * Data to be stored at the node
   */
  final case class Movie(name: String, size: Int, genre: String)

  /**
   * Defines the behavior of the node actor
   */
  /*def process(movies: Set[Movie], coordinates: Array[Array[Int]], neighbours: Map[ActorRef[Command], Array[Array[Int]]],
              transitiveNeighbourMap: Map[ActorRef[Command], Array[Array[Int]]]): Behavior[Command] = Behaviors.receive {
    case (context, NotifyMyNeighbours) =>

    case (context, SplitResponse(coordinates, neighboursMap)) =>

    case (context, Join(replyTo, splitAxis)) =>

    case (context, NotifyNeighboursWithNeighbourMap(neighbourRef, neighbourMap)) =>

    case (context, NotifyNeighboursToUpdateNeighbourMaps(neighbourRef, coordinates)) =>
  }*/
}
