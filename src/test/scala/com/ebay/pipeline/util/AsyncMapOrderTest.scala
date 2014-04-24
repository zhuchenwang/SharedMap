package com.ebay.pipeline.util

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{ActorRef, Props, Actor, ActorSystem}
import akka.pattern._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}
import scala.util.Try
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by zhuwang on 4/23/14.
 */
class AsyncMapOrderTest  extends TestKit(ActorSystem())
  with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll() {
    system.shutdown()
  }

  lazy val handlers = List(
    DummyHandler("com.ebay.pipeline.util.dummyHandlerA", 1),
    DummyHandler("com.ebay.pipeline.util.dummyHandlerB", 2),
    DummyHandler("com.ebay.pipeline.util.dummyHandlerC", 3),
    DummyHandler("com.ebay.pipeline.util.dummyHandlerD", 4)
  )

  "AsyncMap" should "support sequential read and write" in {
    val mgr = system actorOf Props(classOf[DummyMgr], handlers.reverse)
    mgr ! Start
    expectMsg(Map("A" -> 3, "B" -> 2, "C" -> 100))
  }

}

case class DummyHandler(className: String, sequence: Int = 0)

case object Start
case object Done

class DummyMgr(handlers: List[DummyHandler]) extends Actor {

  val actors = handlers map {case DummyHandler(className, sequence) =>
    context actorOf (Props(Class.forName(className), handlers sortWith(_.sequence > _.sequence)))
  }

  implicit val system = context.system
  implicit val operator = this

  val map = AsyncMap.empty[String, Int]()

  private var processed: Int = 0

  private var client: ActorRef = ActorRef.noSender

  def receive: Receive = {
    case Start => actors foreach (_ ! map)
      client = sender

    case Done => processed += 1
      if (processed == handlers.size) {
        map.map() pipeTo client
      }
  }
}

abstract class handler(handlers: List[DummyHandler]) extends Actor {
  val remainList = handlers dropWhile (_.className != getClass.getName)
  val curretnNum = Try(remainList.head.sequence) getOrElse 0
  val preNum = Try(remainList.tail.head.sequence) getOrElse 0

  implicit val operator = this
  println(s"${getClass.getName} $preNum, $curretnNum")
}

class dummyHandlerA(val handlers: List[DummyHandler]) extends handler(handlers) {

  // write 1 to A
  def receive: Receive = {
    case map: AsyncMapRef[String, Int] => map set ("A", 1, curretnNum)
      sender ! Done
  }
}

class dummyHandlerB(handlers: List[DummyHandler]) extends handler(handlers) {

  // write 2 to B
  def receive: Receive = {
    case map: AsyncMapRef[String, Int] => map set ("B", 2, curretnNum)
      sender ! Done
  }
}

class dummyHandlerC(handlers: List[DummyHandler]) extends handler(handlers) {

  implicit val timeout: FiniteDuration = 1 second

  // read B write B + 1 to A
  def receive: Receive = {
    case map: AsyncMapRef[String, Int] => val originalSender = sender
      map get ("B", preNum) map (_ + 1) onSuccess {
        case value: Int => map set ("A", value, curretnNum)
          originalSender ! Done
      }
  }
}

class dummyHandlerD(handlers: List[DummyHandler]) extends handler(handlers) {

  // write 100 to C
  def receive: Receive = {
    case map: AsyncMapRef[String, Int] => map set ("C", 100, curretnNum)
      sender ! Done
  }
}
