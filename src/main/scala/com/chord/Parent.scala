package com.chord

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}

import scala.collection.mutable

object Parent {

  trait Command
  final case class Join(successorNodeRef: ActorRef[Server.Command], predecessorNodeRef: ActorRef[Server.Command]) extends Command with Server.Command
  final case object ObserveFingerTable extends Command
  final case class ActorName(name: String) extends Command

  def apply(m: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]]): Behavior[Command] =
    Behaviors.setup[Command] (context => {
      val serverNode1 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
        null, null), "Server1")
      slotToAddress += (0 -> serverNode1)
      serverNode1 ! Join(serverNode1, findPredecessor(m, 0, slotToAddress, context))

      val serverNode2 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
      null, null), "Server2")
      slotToAddress += (1 -> serverNode2)
      serverNode2 ! Join(findExistingSuccessorNode(m, 1, slotToAddress, context), findPredecessor(m, 1, slotToAddress, context))
      Behaviors.same
    })

  /**
   * Cyclically go through all entries and find the predecessor node of the new node joining
   */
  def findPredecessor(m: Int, n: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]],
                     context: ActorContext[Command]): ActorRef[Server.Command] = {
    slotToAddress.keySet.toList.sorted.foreach(key => {
      if (((key+1) % Math.pow(2, m)) == n) {
        context.log.info(s"predecessor node = ${slotToAddress(key)}")
        return slotToAddress(key)
      }
    })
    context.log.info(s"predecessor node = ${slotToAddress(n)}")
    slotToAddress(n)
  }

  /**
   * Cyclically go through all entries in the slotToAddress mapping table and find the first successor node of new node n
   */
  def findExistingSuccessorNode(m: Int, n: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]],
                               context: ActorContext[Command]): ActorRef[Server.Command] = {
    /*println(slotToAddress.toString())
    var result: ActorRef[Server.Command] = null
    ((n+1) until (2 ^ m)).foreach(i =>
      if (slotToAddress.contains(i)) {
        context.log.info(s"Found successor node at slot number $i")
        result = slotToAddress(i)
      }
    )
    if (result == null) {
      (0 until n).foreach(i =>
        if (slotToAddress.contains(i)) {
          context.log.info(s"Found successor node at slot number $i")
          result = slotToAddress(i)
        })
    }
    if (result == null) {
      context.log.info(s"could not find successor node. new node is the first node to join the ring.")
      slotToAddress(n)
    }
    else {
      context.log.info(s"Successor node found is $result")
      result
    }*/
    val resultIndex = slotToAddress.keySet.toList.sorted.find(_ > n)
    if (resultIndex.isEmpty) {
      context.log.info(s"successor node = ${slotToAddress(slotToAddress.keySet.toList.sorted.find(_ <= n).get)}")
      slotToAddress(slotToAddress.keySet.toList.sorted.find(_ <= n).get)
    }
    else {
      context.log.info(s"successor node = ${slotToAddress(resultIndex.get)}")
      slotToAddress(resultIndex.get)
    }
  }
}
