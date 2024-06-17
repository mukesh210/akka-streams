package part5_advanced

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, Graph, KillSwitches, UniqueKillSwitch}

import scala.concurrent.duration._

object DynamicStreamHandling extends App {

  implicit val system = ActorSystem("DynamicStreamHandling")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  //#1:  Kill Switch
  val killSwitchFlow: Graph[FlowShape[Int, Int], UniqueKillSwitch] = KillSwitches.single[Int]

  val counter = Source(Stream.from(1)).throttle(1, 1 second)
    .log("counter")
  val sink = Sink.ignore

//   single killswitch
//  val killSwitch = counter
//    .viaMat(killSwitchFlow)(Keep.right)
//    .toMat(sink)(Keep.left)
//    .run()
//
//  system.scheduler.scheduleOnce(3 seconds) {
//    killSwitch.shutdown()
//  }

  // shared kill switch
  val anotherCounter = Source(Stream.from(1)).throttle(2, 1 second).log("anotherCounter")
  val sharedKillSwitch = KillSwitches.shared("oneButtonToRuleThemAll")

//  counter.via(sharedKillSwitch.flow).runWith(Sink.ignore)
//  anotherCounter.via(sharedKillSwitch.flow).runWith(Sink.ignore)
//
//  system.scheduler.scheduleOnce(3 seconds) {
//    sharedKillSwitch.shutdown()
//  }

  // dynamic publishers and subscribers
  // MergeHub
  val dynamicMerge: Source[Int, Sink[Int, NotUsed]] = MergeHub.source[Int]
 // val materializedSink: Sink[Int, NotUsed] = dynamicMerge.to(Sink.foreach[Int](println)).run()

  // use this sink any time we like
//  Source(1 to 10).runWith(materializedSink)
//  counter.runWith(materializedSink)

  // BroadcastHub
//  val dynamicBroadcast: Sink[Int, Source[Int, NotUsed]] = BroadcastHub.sink[Int]
//  val materializedSource: Source[Int, NotUsed] = Source(1 to 3).log("broadcastHubSource").runWith(dynamicBroadcast)
//
//  materializedSource.runWith(Sink.ignore)
//  materializedSource.runWith(Sink.foreach[Int](println))

  /**
    * Challenge: combine MergeHub and a broadcastHub
    * MergeHub -> BroadcastHub is a publisher-subscriber model where
    * subscriber will receive all changes from publisher
    *
    * A publisher-subscriber component
    */
    // mergeHub is the publisher to which multiple components can publish
  val merge: Source[String, Sink[String, NotUsed]] = MergeHub.source[String]
  // BroadcastHub is the Subscriber to which multiple components can subscribe
  val bcast: Sink[String, Source[String, NotUsed]] = BroadcastHub.sink[String]
  val (publisherPort, subscriberPort) = merge.toMat(bcast)(Keep.both).run()

  subscriberPort.runWith(Sink.foreach[String](e => println(s"I received: ${e}")))
  subscriberPort.map(string => string.length).runWith(Sink.foreach(n => println(s"I got a number: ${n}")))

  Source(List("Akka", "is", "amazing")).runWith(publisherPort)
  Source(List("I", "love", "scala")).runWith(publisherPort)
  Source.single("Streammmmmmssss").runWith(publisherPort)
}
