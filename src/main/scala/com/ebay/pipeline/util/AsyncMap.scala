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

  private val actualMap = mutable.HashMap.empty[K, UpdatedValue[V]]

  private val subscriptions = mutable.HashMap.empty[K, mutable.ListBuffer[Subscriber]]

  def receive: Receive = {
    case Get(key: K, preNum, timestamp) => actualMap get key match {
        case Some(value) if value.updated >= preNum => sender ! Value(value.value, timestamp)
        case _ => subscribe(key, Subscriber(sender, preNum, timestamp))
      }

    // only set the value if current number is after the existing number
    case Set(key: K, value: V, currentNum) if (actualMap get key map {_.updated} getOrElse 0) <= currentNum =>
      //println(s"set $key -> $value: $currentNum")
      actualMap update(key, UpdatedValue(value, currentNum))
      publish(key, UpdatedValue(value, currentNum))

    // only remove the value if current number is after the existing number
    case Remove(key: K, currentNum, timestamp) if (actualMap get key map {_.updated} getOrElse 0) <= currentNum =>
      sender ! ((actualMap remove key) map (_.value), timestamp)

    case Clear => actualMap.clear
      subscriptions.clear

    case EntireMap => sender ! (actualMap map {case (key, value) => key -> value.value}).toMap

    case _ =>
  }

  private def subscribe(key: K, subscriber: Subscriber) {
    subscriptions get key match {
      case Some(subscribers) => subscribers += subscriber
      case None => subscriptions += (key -> mutable.ListBuffer(subscriber))
    }
  }

  private def publish(key: K, updatedValue: UpdatedValue[V]) {
    subscriptions get key foreach {subscribers =>
      val (ready, notReady) = subscribers partition {_.preNum <= updatedValue.updated}
      ready foreach {subscriber => subscriber.actor ! Value(updatedValue.value, subscriber.timestamp)}
      subscriptions update (key, notReady)
    }
  }

}
