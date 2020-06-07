package part4_techniques

import java.util.Date

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

object AdvancedBackpressure extends App {
  implicit val system = ActorSystem("AdvancedBackpressure")
  implicit val materializer = ActorMaterializer()

  /*
    If consumer is slow, we can apply backpressure in the Flow by
    using buffer... this is standard approach
    but what if due to some reason, Source/Sink can not be backpressured
   */
  val controlledFlow = Flow[Int].map(_ * 2).buffer(10, OverflowStrategy.dropHead)

  // `nInstances` introduced when using `conflate`
  case class PagerEvent(description: String, date: Date, nInstances: Int = 1)
  case class Notification(email: String, pagerEvent: PagerEvent)

  val events = List(
    PagerEvent("Service discovery failed", new Date()),
    PagerEvent("Illegal event in the data pipeline", new Date()),
    PagerEvent("Number of HTTP 500 spiked", new Date()),
    PagerEvent("A service stopped responding", new Date())
  )
  val eventSource = Source(events)

  val onCallEngineer = "mukesh@gmail.com" // a fast service for fetching on call engineer

  def sendEmail(notification: Notification) =
    println(s"Dear ${notification.email}, you have an event: ${notification.pagerEvent}") // actually send an email

  val notificationSink = Flow[PagerEvent].map(event => Notification(onCallEngineer, event))
    .to(Sink.foreach[Notification](sendEmail))

  // standard
  // eventSource.to(notificationSink).run()

  /*
    Slower Consumer but fast Producer, we do not want to use backpressure
    un-backpressurable source:
      Suppose for some reason, our source is timer based and can not be backpressured
    Solution: aggregate events and send them to Sink when it demands
   */
  def sendEmailSlow(notification: Notification) = {
    Thread.sleep(1000)
    println(s"Dear ${notification.email}, you have an event: ${notification.pagerEvent}") // actually send an email
  }

  val aggregateNotificationFlow = Flow[PagerEvent]
    .conflate((event1, event2) => { // similar to fold, but emits element only when sink demands
      val nInstances = event1.nInstances + event2.nInstances
      PagerEvent(s"You have $nInstances events that require your attention", new Date(), nInstances)
    })
    .map(resultingEvent => Notification(onCallEngineer, resultingEvent))

  // conflate: alternative to backpressure
  // eventSource.via(aggregateNotificationFlow).async.to(Sink.foreach[Notification](sendEmailSlow)).run()

  /*
    Slow Producers: extrapolate/expand

    extrapolate: in the mean time when no values are produced but Sink is waiting,
      function passed in `extrapolate` will calculate new values and pass them to Sink.
      Will create Iterator when the demand is not met

    expand: similar to extrapolate, but will create Iterator every time
      Will create Iterator everytime
   */
  import scala.concurrent.duration._
  val slowCounter = Source(Stream.from(1)).throttle(1, 1 second)
  val hungrySink = Sink.foreach[Int](println)

  val extrapolator = Flow[Int].extrapolate(element => Iterator.from(element))
  val repeater = Flow[Int].extrapolate(element => Iterator.continually(element))

  slowCounter.via(repeater).to(hungrySink).run()

  val expander = Flow[Int].expand(element => Iterator.from(element))
}
