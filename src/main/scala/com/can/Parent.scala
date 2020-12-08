package com.can

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Random

/**
 * This parent actor acts as the top-level actor in the Content Addressable Network.
 * It is responsible for finding a random existing node in the CAN, whose zone is to be
 * split with a new node, and make random joins and failures of nodes.
 */
object Parent {
  trait Command

  /**
   * Message for getting a random node from the CAN
   */
  final case class GetExistingNode(replyTo: ActorRef[Node.Command]) extends Command

  /**
   * Messages for handling movie storage/retrieval requests
   */
  final case class FindNodeForStoringData(replyTo: ActorRef[Node.DataActionResponse], name: String, size: Int, genre: String) extends Command
  final case class FindSuccessorForFindingData(replyTo: ActorRef[Node.DataActionResponse], name: String) extends Command

  /**
   * Message for terminating or creating a random node in the CAN
   */
  final case object TerminateOrJoinNode extends Command

  final case class DumpNodeState(replyTo: ActorRef[Command]) extends Command with Node.Command
  final case class NodeState(node: ActorRef[Node.Command], state: Node.State) extends Command

  def apply(n: Int, endX: Int, endY: Int, replicationPeriod: Int, joinFailPeriod: Int, dumpPeriod: Int):
  Behavior[Command] = Behaviors.setup{context =>
    val nodes = spawnNodes(n, context, endX, endY, replicationPeriod)

    Behaviors.withTimers { timer =>
//      timer.startTimerWithFixedDelay(TerminateOrJoinNode, joinFailPeriod.seconds)
      timer.startTimerWithFixedDelay(DumpNodeState(context.self), dumpPeriod.seconds)
      process(nodes, nodeJoinFlag=true, axis='X', endX, endY, replicationPeriod)
    }
  }

  def process(nodes: List[ActorRef[Node.Command]], nodeJoinFlag: Boolean, axis: Character, endX: Int, endY: Int,
              replicationPeriod: Int, state: Map[ActorRef[Node.Command], Node.State]=Map.empty): Behavior[Command] = Behaviors.receive {

    case (context, DumpNodeState(ref)) =>
      context.log.info(s"${context.self.path}\t:\tAsking nodes for their states...")
      nodes.foreach(n => n ! DumpNodeState(ref))
      Behaviors.same

    case (context, NodeState(ref, nodeState)) =>
      context.log.info(s"${context.self.path}\t:\tGot state from node $ref")
      if (state.keySet.size == nodes.size-1) {
        context.log.info(s"${context.self.path}\t:\tAll nodes have sent their states. Dumping all states to disk now.")
        dumpState(state+(ref->nodeState))
        process(nodes, nodeJoinFlag, axis, endX, endY, replicationPeriod)
      }
      else process(nodes, nodeJoinFlag, axis, endX, endY, replicationPeriod, state+(ref->nodeState))


    case (context, TerminateOrJoinNode) =>
      if (nodeJoinFlag) {
        val newNode = context.spawn(Node(nodes.size+1, getRandomNode(nodes), axis, replicationPeriod), s"node${Random.nextInt(10000)}")
        context.log.info(s"${context.self.path}\t:\tCreating node $newNode")
        process(nodes:+newNode, nodeJoinFlag=false, if (axis=='X') 'Y' else 'X', endX, endY, replicationPeriod)
      }
      else {
        val randomIndex = Random.nextInt(nodes.size)
        val randomNode = nodes(randomIndex)
        context.log.info(s"${context.self.path}\t:\tStopping node $randomNode")
        randomNode ! Node.Stop
        process(nodes.slice(0, randomIndex)++nodes.slice(randomIndex+1, nodes.size), nodeJoinFlag=true, axis, endX, endY, replicationPeriod)
      }

    case (context, FindNodeForStoringData(replyTo, name, size, genre)) =>
      val randomNode = getRandomNode(nodes)
      context.log.info(s"${context.self.path}\t:\tGot request for storing movie $name. Forwarding request to node $randomNode in the CAN")
      randomNode ! Node.FindNodeForStoringData(replyTo, name, size, genre, endX, endY)
      Behaviors.same

    case (context, FindSuccessorForFindingData(replyTo, name)) =>
      val randomNode = getRandomNode(nodes)
      context.log.info(s"${context.self.path}\t:\tGot request for obtaining movie $name. Forwarding search request to node $randomNode in the CAN")
      randomNode ! Node.FindSuccessorForFindingData(replyTo, name, endX, endY)
      Behaviors.same
  }

  /**
   * This will prefill the CAN with a handful of nodes
   * @param n Number of nodes to be created
   * @param context Execution context of a node actor
   */
  def spawnNodes(n: Int, context: ActorContext[Command], endX: Int, endY: Int, replicationPeriod: Int): List[ActorRef[Node.Command]] = {
    val nodes = mutable.ListBuffer[ActorRef[Node.Command]]()
    var axis = 'X'

    (0 until n).foreach {i =>
      if (i == 0) nodes.append(context.spawn(Node(i, endX, endY, replicationPeriod), s"node$i"))
      else {
        nodes.append(context.spawn(Node(i, getRandomNode(nodes.toList), axis, replicationPeriod), s"node$i"))
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

  def dumpState(nodeStates: Map[ActorRef[Node.Command], Node.State]): Unit = {
    val outputFile = new FileWriter(new File("src/main/resources/outputs/canState.yml"))
    val objectMapper = new ObjectMapper(new YAMLFactory().enable(Feature.INDENT_ARRAYS))
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValue(outputFile, nodeStates)
    outputFile.close()
  }
}
