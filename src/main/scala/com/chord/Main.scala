package com.chord

import akka.actor.typed.{ActorRef, ActorSystem}

import scala.collection.mutable

object Main {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem(Parent(20, mutable.Map[Int, ActorRef[Server.Command]]()), "ChordActorSystem")
//    system.terminate()
  }
}
