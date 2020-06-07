package part4_techniques

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import scala.concurrent.duration._

object IntegratingWithActors extends App {

  implicit val system = ActorSystem("IntegratingWithActor")
  implicit val materializer = ActorMaterializer()

  class SimpleActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case s: String =>
        log.info(s"Just received a string: ${s}")
        sender() ! s"${s}${s}"
      case num: Int =>
        log.info(s"Just received a number: ${num}")
        sender() ! (2 * num)
      case _ =>
    }
  }

  val simpleActor = system.actorOf(Props[SimpleActor], "simpleActor")

  val numberSource = Source(1 to 10)

  // actor as a flow
  implicit val timeout = Timeout(2 seconds)
  // parallelism means if this much messages are already present in mailbox, actor will start backpressuring
  val actorBasedFlow = Flow[Int].ask[Int](parallelism = 4)(simpleActor)

//  numberSource.via(actorBasedFlow).to(Sink.ignore).run() // equivalent to
//  numberSource.ask[Int](parallelism = 4)(simpleActor).to(Sink.ignore).run()

  /*
    Actor as a source:  Source.actorRef
    when using Actor as a source, we will get hold of ActorRef
    We can use this ActorRef to send it message anytime we want
   */
  // materialized value of this source is "ActorRef"
  val actorPoweredSource = Source.actorRef[Int](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
  // materialized value of this expression would be ActorRef because Materialized value of left most component is kept by default
  val materializedActorRef = actorPoweredSource.to(Sink.foreach[Int](num => println(s"Actor powered flow got number: ${num}"))).run()
  materializedActorRef ! 10
  // terminating the stream
  materializedActorRef ! akka.actor.Status.Success("complete")

  /*
    Actor as a destination/sink
      -- an init message
      -- an ack message to confirm the reception
      -- a complete message
      -- a function to generate a message in case the stream throws an exception
   */
  case object StreamInit
  case object StreamAck
  case object StreamComplete
  case class StreamFail(ex: Throwable)

  class DestinationActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case StreamInit =>
        log.info("Stream initialized")
        sender() ! StreamAck
      case StreamComplete =>
        log.info("Stream complete")
        context.stop(self)
      case StreamFail(ex) =>
        log.warning(s"Stream failed: ${ex}")
      case message =>
        log.info(s"Message: ${message} has come to its final resting point.")
        sender() ! StreamAck  // need to reply with StreamAck, otherwise it will be interpreted as backpressure
    }
  }

  val destinationActor = system.actorOf(Props[DestinationActor], "destinationActor")

  val actorPoweredSink = Sink.actorRefWithAck(
    destinationActor,
    onInitMessage = StreamInit,
    onCompleteMessage = StreamComplete,
    ackMessage = StreamAck,
    onFailureMessage = throwable => StreamFail(throwable) // optional
  )

  Source(1 to 10).to(actorPoweredSink).run()

  // another solution -- not recommended... not capable to backpressuring
  // Sink.actorRef()
}
