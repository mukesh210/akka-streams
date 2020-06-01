package part2_primer

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

// backpressure is all about slowing down fast producer in presence of slow consumer
// in Akka streams... consumer drives the flow
object BackpressureBasics extends App {

  implicit val system = ActorSystem("BackpressureBasics")
  implicit val materializer = ActorMaterializer()

  val fastSource = Source(1 to 1000)
  val slowSink = Sink.foreach[Int]{x =>
    Thread.sleep(1000)
    println(s"Sink: ${x}")
  }

  // not backpressure because all messages are processed on same actor
  // fastSource.to(slowSink).run() // fusing?!

  // backpressure happens here
  // fastSource.async.to(slowSink).run()

  val simpleFlow = Flow[Int].map{ x =>
    println(s"Incoming: ${x}")
    x + 1
  }

  // sink will buffer 16 messages, once sink it consumes 8 of them, simpleFlow again fetches 8 more messages
//  fastSource.async
//    .via(simpleFlow).async
//    .to(slowSink).run()

  /*
    component reaction to backpressure
      - try to slow down if possible
      - buffer elements until there is more demand
      - drop down elements from the buffer if it overflow
      - tear down/kill the whole stream(failure)
   */

  val bufferedFlow = simpleFlow.buffer(10, overflowStrategy = OverflowStrategy.dropHead)
//  fastSource.async
//    .via(bufferedFlow).async
//    .to(slowSink).run()
  /*
    1 - 16: nobody is backpressured(buffered in sink)
    17-26: flow will buffer, flow will start dropping
    26-1000: flow will always drop the oldest element
      => 991 - 1000 => 992 - 1001 => sink
   */
  /*
    overflow strategy
      - drop head = oldest
      - drop tail = newest
      - drop new = exact element to be added = keeps the buffer
      - drop the entire buffer
      - backpressure signal
      - fail
   */

  // throttling
  import scala.concurrent.duration._
  fastSource.throttle(2, 1 second).runWith(Sink.foreach(println))
}
