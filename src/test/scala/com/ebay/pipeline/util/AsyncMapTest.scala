package com.ebay.pipeline.util

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}
import scala.concurrent.duration._
import akka.pattern._
import scala.concurrent.Future
import akka.actor.Status.Failure

/**
 * Created by zhuwang on 4/15/14.
 */
class AsyncMapTest extends TestKit(ActorSystem())
  with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll() {
    system.shutdown()
  }

  "Constructor" should "return a reference of empty map" in {
    val mapRef = AsyncMap[String, Int]()
    val getter = system actorOf Props(classOf[Getter[String, Int]], mapRef)

    getter ! "key"
    expectMsgClass(classOf[Failure])

    getter ! PoisonPill
    mapRef.destroy
  }

  "empty method" should "return a reference of empty map" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val getter = system actorOf Props(classOf[Getter[String, Int]], mapRef)

    getter ! "key"
    expectMsgClass(classOf[Failure])

    getter ! PoisonPill
    mapRef.destroy
  }

  "get method" should "get the value set by others" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val getter = system actorOf Props(classOf[Getter[String, Int]], mapRef)
    val setter = system actorOf Props(classOf[Setter[String, Int]], mapRef)

    setter ! ("key", 1)
    getter ! "key"
    expectMsg(1)

    getter ! PoisonPill
    setter ! PoisonPill
    mapRef.destroy
  }

  "get method" should "also get the value set by others within timeout" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val getter = system actorOf Props(classOf[Getter[String, Int]], mapRef)
    val setter = system actorOf Props(classOf[Setter[String, Int]], mapRef)

    getter ! "key"
    setter ! ("key", 1)
    expectMsg(1)

    getter ! PoisonPill
    setter ! PoisonPill
    mapRef.destroy
  }

  "remove method" should "return None if the entry does not exist" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val remover = system actorOf Props(classOf[Remover[String, Int]], mapRef)

    remover ! "key"
    expectMsgClass(classOf[Failure])

    remover ! PoisonPill
    mapRef.destroy
  }

  "remove method" should "remove an entry and return the value" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val getter = system actorOf Props(classOf[Getter[String, Int]], mapRef)
    val setter = system actorOf Props(classOf[Setter[String, Int]], mapRef)
    val remover = system actorOf Props(classOf[Remover[String, Int]], mapRef)

    setter ! ("key", 1)
    getter ! "key"
    expectMsg(1)

    remover ! "key"
    expectMsg(1)

    getter ! "key"
    expectMsgClass(classOf[Failure])

    getter ! PoisonPill
    setter ! PoisonPill
    remover ! PoisonPill
    mapRef.destroy
  }

  "get method" should "get the different values before and after a set" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val getter = system actorOf Props(classOf[Getter[String, Int]], mapRef)
    val setter = system actorOf Props(classOf[Setter[String, Int]], mapRef)
    val remover = system actorOf Props(classOf[Remover[String, Int]], mapRef)

    getter ! "key"
    setter ! ("key", 1)
    expectMsg(1)

    remover ! "key"
    expectMsg(1)

    setter ! ("key", 2)
    getter ! "key"
    expectMsg(2)

    getter ! PoisonPill
    setter ! PoisonPill
    remover ! PoisonPill
    mapRef.destroy
  }

  "Getter" should "get the value twice if it calls get twice" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val getter = system actorOf Props(classOf[Getter[String, Int]], mapRef)
    val setter = system actorOf Props(classOf[Setter[String, Int]], mapRef)
    val remover = system actorOf Props(classOf[Remover[String, Int]], mapRef)

    setter ! ("key", 1)
    getter ! "key"
    getter ! "key"
    expectMsg(1)
    expectMsg(1)

    remover ! "key"
    expectMsg(1)

    getter ! "key"
    setter ! ("key", 2)
    getter ! "key"
    expectMsg(2)
    expectMsg(2)

    remover ! "key"
    expectMsg(2)

    getter ! "key"
    getter ! "key"
    setter ! ("key", 3)
    expectMsg(3)
    expectMsg(3)

    getter ! PoisonPill
    setter ! PoisonPill
    remover ! PoisonPill
    mapRef.destroy
  }

  "Aggregator" should "aggregate the result" in {
    val mapRef = AsyncMap.empty[String, Int]()
    val setter1 = system actorOf Props(classOf[Setter[String, Int]], mapRef)
    val setter2 = system actorOf Props(classOf[Setter[String, Int]], mapRef)
    val aggregator = system actorOf Props(classOf[Aggregator[String]], mapRef)

    setter1 ! ("a", 3)
    setter2 ! ("b", 4)
    aggregator ! Seq("a", "b")
    expectMsg(7)

    setter1 ! PoisonPill
    setter2 ! PoisonPill
    aggregator ! PoisonPill
    mapRef.destroy
  }

  "AsyncMapRef" should "be shared through message passing" in {
    import TestActors._
    val mapRef = AsyncMap.empty[String, Int]()
    val a1 = system actorOf Props[TestActor]
    val a2 = system actorOf Props[TestActor]
    val a3 = system actorOf Props[TestActor]

    a1 ! TestActors.Get(mapRef, "a")
    a2 ! TestActors.Set(mapRef, "a", 1)
    expectMsg(1)

    a3 ! TestActors.Remove(mapRef, "a")
    expectMsg(1)

    a3 ! TestActors.Set(mapRef, "b", 2)
    a2 ! TestActors.Get(mapRef, "b")
    a1 ! TestActors.Get(mapRef, "b")
    expectMsg(2)
    expectMsg(2)

    a2 ! TestActors.Clear(mapRef)
    Thread.sleep(1000)
    a3 ! TestActors.Get(mapRef, "b")
    expectMsgClass(classOf[Failure])
    a1 ! TestActors.Get(mapRef, "b")
    expectMsgClass(classOf[Failure])

    a1 ! PoisonPill
    a2 ! PoisonPill
    a3 ! PoisonPill
    mapRef.destroy
  }

}

object TestActors {

  case class Get[K, V](map: AsyncMapRef[K, V], key: K)
  case class Set[K, V](map: AsyncMapRef[K, V], key: K, value: V)
  case class Remove[K, V](map: AsyncMapRef[K, V], key: K)
  case class Clear[K, V](map: AsyncMapRef[K, V])

  class TestActor extends Actor {

    implicit val operator = this
    implicit val timeout: FiniteDuration = 1 second
    implicit val executionContext = context.dispatcher

    def receive: Receive = {
      case Get(map, key) => map get key pipeTo sender
      case Set(map, key, value) => map set (key, value)
      case Remove(map, key) => map remove key pipeTo sender
      case Clear(map) => map.clear()
      case _ =>
    }
  }

}

class Getter[K, V](map: AsyncMapRef[K, V]) extends Actor {

  implicit val operator = this
  implicit val timeout: FiniteDuration = 1 second
  implicit val executionContext = context.dispatcher

  def receive: Receive = {
    case key: K => map.get(key) pipeTo sender
  }
}

class Setter[K, V](map: AsyncMapRef[K, V]) extends Actor {

  implicit val operator = this
  implicit val timeout: FiniteDuration = 1 second

  def receive: Receive = {
    case (key: K, value: V) => map set (key, value)
  }
}

class Remover[K, V](map: AsyncMapRef[K, V]) extends Actor {

  implicit val operator = this
  implicit val timeout: FiniteDuration = 1 second
  implicit val executionContext = context.dispatcher

  def receive: Receive = {
    case key: K => map.remove(key) pipeTo sender
  }
}

class Aggregator[K](map: AsyncMapRef[K, Int]) extends Actor {

  implicit val operator = this
  implicit val timeout: FiniteDuration = 1 second
  implicit val executionContext = context.dispatcher

  private var originalSender: ActorRef = context.system.deadLetters

  def receive: Receive = {
    case keys: Seq[K] => Future sequence (keys map (map get _)) map (_.sum) pipeTo self
      originalSender = sender

    case sum: Int => originalSender ! sum

    case t: Failure => originalSender ! t
  }

}
