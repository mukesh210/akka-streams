package part4_techniques

import akka.actor.ActorSystem
import akka.stream.Supervision.{Resume, Stop}
import akka.stream.{ActorAttributes, ActorMaterializer}
import akka.stream.scaladsl.{RestartSource, Sink, Source}

import scala.concurrent.duration._
import scala.util.Random

object FaultTolerance extends App {
  implicit val system = ActorSystem("FaultTolerance")
  implicit val materializer = ActorMaterializer()

  // 1 - logging
  val faultySource = Source(1 to 10).map(e => if(e == 3) throw new RuntimeException else e)
  faultySource.log("trackingElements").to(Sink.ignore)
    //.run()
  /*
    When an operator fails, all upstreams are cancelled and all downstreams are signalled
   */

  // 2 - gracefully terminating a stream
  // Stream will close without failing
  faultySource.recover {
    case _ : RuntimeException => Int.MinValue
  }.log("gracefulSource")
    .to(Sink.ignore)
    //.run()

  // 3 - recover with another stream
  faultySource.recoverWithRetries(3, {
    case _ : RuntimeException => Source(90 to 93)
  })
    .log("recoverWithRetries")
    .to(Sink.ignore)
//     .run()

  // 4 - backoff supervision
  val restartSource = RestartSource.onFailuresWithBackoff(
    minBackoff = 1 second,
    maxBackoff = 30 seconds,
    randomFactor = 0.2,
  )(() => {
    val randomNumber = new Random().nextInt(20)
    Source(1 to 10).map(elem => if(elem == randomNumber) throw new RuntimeException else elem)
  })
    .log("restartBackoff")
    .to(Sink.ignore)
//     .run()

  // 5 - supervision strategy
  // Akka streams operators by default fail... they are not bounded by supervision strategy
  val numbers = Source(1 to 10).map(n => if(n == 5) throw new RuntimeException("bad luck") else n).log("supervision")
  val supervisedNumbers = numbers.withAttributes(ActorAttributes.supervisionStrategy {
    // Resume: Skips faulty element,
    // Stop: stop the stream,
    // Restart: resume + clears the internal state of component
    case _: RuntimeException => Resume
    case _ => Stop
  })

  supervisedNumbers.to(Sink.ignore).run()
}
