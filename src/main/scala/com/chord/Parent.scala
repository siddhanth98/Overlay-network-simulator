package com.chord

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import scala.collection.mutable

object Parent {

  trait Command
  final case class Join(siblingRef: ActorRef[Server.Command]) extends Command with Server.Command
  final case object ObserveFingerTable extends Command
  final case class ActorName(name: String) extends Command

  def apply(m: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]]): Behavior[Command] =
    Behaviors.setup[Command] (context => {
      val serverNode1 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
        null, null), "Server1")
      slotToAddress += (0 -> serverNode1)
      serverNode1 ! Join(serverNode1)

      val serverNode2 = context.spawn(Server(m, mutable.Map[Int, Int](), mutable.Map[Int, ActorRef[Server.Command]](),
      null, null), "Server2")
      slotToAddress += (1 -> serverNode2)
      serverNode2 ! Join(findExistingSuccessorNode(m, 1, slotToAddress))
      Behaviors.same
    })

  /**
   * Cyclically go through all entries in the slotToAddress mapping table and find the first successor node of new node n
   */
  def findExistingSuccessorNode(m: Int, n: Int, slotToAddress: mutable.Map[Int, ActorRef[Server.Command]]): ActorRef[Server.Command] = {
    println(slotToAddress.toString())
    var result: ActorRef[Server.Command] = null
    ((n+1) until (2 ^ m)).foreach(i =>
      if (slotToAddress.contains(i)) {
        println(s"Found successor node at slot number $i")
        result = slotToAddress(i)
      }
    )
    if (result == null) {
      (0 until n).foreach(i =>
        if (slotToAddress.contains(i)) {
          println(s"Found successor node at slot number $i")
          result = slotToAddress(i)
        })
    }
    if (result == null) {
      println(s"could not find successor node. new node is the first node to join the ring.")
      slotToAddress(n)
    }
    else {
      println(result)
      result
    }
  }
}
