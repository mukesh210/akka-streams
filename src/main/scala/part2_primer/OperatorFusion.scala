package part2_primer

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

object OperatorFusion extends App {

  implicit val system = ActorSystem("OperatorFusion")
  implicit val materializer = ActorMaterializer()

  val simpleSource = Source(1 to 1000)
  val simpleFlow = Flow[Int].map(_ + 1)
  val simpleFlow2 = Flow[Int].map(_ * 10)
  val simpleSink = Sink.foreach[Int](println)

  // akka stream components are based on Actors
  // this runs on the SAME ACTOR
  // simpleSource.via(simpleFlow).via(simpleFlow2).to(simpleSink).run()
  // operator/component FUSION  = connecting component using `via` and `to`
  // one element will be processed completely before another one is sent for processing
  // operators are fused with Source... that's why OPERATOR FUSION

  // "equivalent" behavior
  class SimpleActor extends Actor {
    override def receive: Receive = {
      case x: Int =>
        // flow operation
        val x2 = x + 1
        val y = x2 * 10
        // sink operation
        println(y)
    }
  }
  val simpleActor = system.actorOf(Props[SimpleActor])
  // (1 to 1000).foreach(simpleActor ! _)

  // operator fusion are sometimes more expensive when flow take sometime for processing
  // complex flows
  val complexFlow = Flow[Int].map {x =>
    Thread.sleep(1000)
    x + 1
  }
  val complexFlow2 = Flow[Int].map {x =>
    Thread.sleep(1000)
    x * 10
  }

  // simpleSource.via(complexFlow).via(complexFlow2).to(simpleSink).run()

  // async boundary
  /*simpleSource.via(complexFlow).async // runs on one actor
    .via(complexFlow2).async  // runs on other actor
    .to(simpleSink) // runs on third actor
    .run()
*/
  /**
    * ordering guarantees
    * when no async: Since all messages are processed by same actor... we are sure that
    * Actors will print message in sequence
    *
    * when async: Since, now messages are being processed on different actors,
    * we are sure that 1 will be processed before 2 by Flow B
    */
  Source(1 to 3)
    .map(element => { println(s"Flow A: ${element}"); element }).async
    .map(element => { println(s"Flow B: ${element}"); element }).async
    .map(element => { println(s"Flow C: ${element}"); element }).async
    .runWith(Sink.ignore)
}
