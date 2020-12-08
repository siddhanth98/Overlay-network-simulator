package com.can

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Random

/**
 * This parent actor acts as the top-level actor in the Content Addressable Network
 * It is responsible to find a random existing node in the CAN, whose zone is to be
 * split with a new node
 */
object Parent {
  trait Command

  /**
   * Message for getting a random node from the CAN
   */
  final case class GetExistingNode(replyTo: ActorRef[Node.Command]) extends Command

  final case object TerminateOrJoinNode extends Command

  def apply(n: Int): Behavior[Command] = Behaviors.setup{context =>
    val nodes = spawnNodes(n, context)

    Behaviors.withTimers { timer =>
      timer.startTimerWithFixedDelay(TerminateOrJoinNode, 20.seconds)
      process(nodes, nodeJoinFlag=true, axis='X')
    }
  }

  def process(nodes: List[ActorRef[Node.Command]], nodeJoinFlag: Boolean, axis: Character): Behavior[Command] = Behaviors.receive {
    case (context, TerminateOrJoinNode) =>
      if (nodeJoinFlag) {
        val newNode = context.spawn(Node(nodes.size+1, getRandomNode(nodes), axis), s"node${Random.nextInt(10000)}")
        context.log.info(s"${context.self.path}\t:\tCreating node $newNode")
        process(nodes:+newNode, nodeJoinFlag=false, if (axis=='X') 'Y' else 'X')
      }
      else {
        val randomIndex = Random.nextInt(nodes.size)
        val randomNode = nodes(randomIndex)
        context.log.info(s"${context.self.path}\t:\tStopping node $randomNode")
        randomNode ! Node.Stop
        process(nodes.slice(0, randomIndex)++nodes.slice(randomIndex+1, nodes.size), nodeJoinFlag=true, axis)
      }
  }

  /**
   * This will prefill the CAN with a handful of nodes
   * @param n Number of nodes to be created
   * @param context Execution context of a node actor
   */
  def spawnNodes(n: Int, context: ActorContext[Command]): List[ActorRef[Node.Command]] = {
    val nodes = mutable.ListBuffer[ActorRef[Node.Command]]()
    var axis = 'X'

    (0 until n).foreach {i =>
      if (i == 0) nodes.append(context.spawn(Node(i), s"node$i"))
      else {
        nodes.append(context.spawn(Node(i, getRandomNode(nodes.toList), axis), s"node$i"))
        if (axis == 'X') axis = 'Y'
        else axis = 'X'
        Thread.sleep(500)
      }
    }
    nodes.toList
  }

  /**
   * Gets a random node already existing in the CAN
   * @param nodes List of nodes already created in the CAN
   */
  def getRandomNode(nodes: List[ActorRef[Node.Command]]): ActorRef[Node.Command] =
    nodes(Random.nextInt(nodes.size))
}
