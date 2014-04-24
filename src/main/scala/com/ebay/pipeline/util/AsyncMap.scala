package com.ebay.pipeline.util

import akka.actor._
import scala.collection.mutable

/**
 * Created by zhuwang on 4/15/14.
 */

object AsyncMap {

  def apply[K, V]()(implicit system: ActorSystem, owner: ActorRef): AsyncMapRef[K, V] = {
    val mapActorRef = system actorOf Props(classOf[AsyncMap[K, V]], owner)
    new AsyncMapRef[K, V](mapActorRef)
  }

  def empty[K, V]()(implicit system: ActorSystem, owner: ActorRef): AsyncMapRef[K, V] = {
    val mapActorRef = system actorOf Props(classOf[AsyncMap[K, V]], owner)
    new AsyncMapRef[K, V](mapActorRef)
  }

}

private class AsyncMap[K, V](owner: ActorRef) extends Actor with ActorLogging {

  private val map = mutable.HashMap.empty[K, V]

  private val subscriptions = mutable.HashMap.empty[K, mutable.ListBuffer[Subscriber]]

  def receive: Receive = {
    case Get(key: K, timestamp) => map get key match {
        case Some(value) => sender ! Value(value, timestamp)
        case None => subscribe(key, Subscriber(sender, timestamp))
      }

    case Set(key: K, value: V) => map update(key, value)
      publish(key, value)

    case Remove(key: K, timestamp) => sender ! ((map remove key), timestamp)

    case Clear => map.clear
      subscriptions.clear

    case EntireMap => sender ! map.toMap

    case _ =>
  }

  private def subscribe(key: K, subscriber: Subscriber) {
    subscriptions get key match {
      case Some(subscribers) => subscribers += subscriber
      case None => subscriptions += (key -> mutable.ListBuffer(subscriber))
    }
  }

  private def publish(key: K, value: V) {
    subscriptions get key foreach {subscribers =>
      subscribers foreach {subscriber => subscriber.actor ! Value(value, subscriber.timestamp)}
      subscribers.clear()
    }
  }

}
