package com.ebay.pipeline

import akka.actor.ActorRef

/**
 * Created by zhuwang on 4/22/14.
 */
package object util {

  case class Get[K](key: K, preNum: Int, timestamp: Long)
  case class Set[K, V](key: K, value: V, currentNum: Int)
  case class Remove[K](key: K, currentNum: Int, timestamp: Long)
  case class Value[V](value: V, timestamp: Long)
  case class ValueNA[K](key: K, timestamp: Long)
  case object Clear
  case object EntireMap

  case class Subscriber(actor: ActorRef, preNum: Int, timestamp: Long)

  case class UpdatedValue[V](value: V, updated: Int)

}
