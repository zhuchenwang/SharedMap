package com.ebay.pipeline.util

import akka.actor.{PoisonPill, Actor, ActorRef}
import akka.actor.Actor._
import scala.concurrent.{Promise, Future}
import scala.util.Try
import scala.concurrent.duration.FiniteDuration

/**
 * Created by zhuwang on 4/22/14.
 */
case class AsyncMapRef[K, V](mapActorRef: ActorRef) {
  import scala.concurrent.ExecutionContext.Implicits.global

  private var behaviorStack = List.empty[Receive]

  def map()(implicit operator: Actor): Future[Map[K, V]] = {
    val promise = Promise[Map[K, V]]()
    mapActorRef tell (EntireMap, operator.self)
    val getMap: Receive = {
      case map: Map[K, V] => popBehavior()
        promise complete Try(map)
    }
    pushBehavior(getMap)
    return promise.future
  }

  def get(key: K, preNum: Int = 0)(implicit operator: Actor, timeout: FiniteDuration): Future[V] = {
    val promise = Promise[V]()
    val timestamp = System.nanoTime
    mapActorRef tell (Get(key, preNum, timestamp), operator.self)
    val alarm = operator.context.system.scheduler.scheduleOnce(timeout, operator.self, ValueNA(key, timestamp))
    val getValue: Receive = {
      case Value(value, timestamp) => alarm.cancel()
        popBehavior()
        promise success value.asInstanceOf[V]

      case ValueNA(key, timestamp) => popBehavior()
        promise failure new Throwable(s"Getting $key: $timestamp timed out")
    }
    pushBehavior(getValue)
    return promise.future
  }

  def set(key: K, value: V, currentNum: Int = 0)(implicit operator: Actor): Unit = {
    mapActorRef tell (Set(key, value, currentNum), operator.self)
  }

  def remove(key: K, currentNum: Int = 0)(implicit operator: Actor): Future[V] = {
    val promise = Promise[V]()
    val timestamp = System.nanoTime
    mapActorRef tell (Remove(key, currentNum, timestamp), operator.self)
    val removeValue: Receive = {
      case (opt: Option[V], timestamp) => promise complete Try(opt.get)
        popBehavior()
    }
    pushBehavior(removeValue)
    return promise.future
  }

  def clear()(implicit operator: Actor): Unit = {
    mapActorRef tell (Clear, operator.self)
  }

  def destroy: Unit = {
    mapActorRef ! PoisonPill
  }

  private def pushBehavior(newBehavior: Receive)(implicit operator: Actor): Unit = {
    behaviorStack = newBehavior :: behaviorStack
    operator.context become (behaviorStack.reduceLeft(_ orElse _) orElse operator.receive, false)
  }

  private def popBehavior()(implicit operator: Actor): Unit = {
    if (!behaviorStack.isEmpty) behaviorStack = behaviorStack.tail
    operator.context.unbecome()
  }
}
