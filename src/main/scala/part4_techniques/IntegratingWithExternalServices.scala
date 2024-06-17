package part4_techniques

import akka.NotUsed

import java.util.Date
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import scala.concurrent.Future

/**
  * Using `mapAsync` to call external services
  */

object IntegratingWithExternalServices extends App {

  implicit val system = ActorSystem("IntegratingWithExternalServices")
  implicit val materializer = ActorMaterializer()
  // import system.dispatcher // not recommended in practice for mapAsync as this might starve akka streams for threads
  implicit val dispatcher = system.dispatchers.lookup("dedicated-dispatcher")

  def genericExtService[A, B](element: A): Future[B] = ???


  // example: simplified PagerDuty: Notify someone when application fails in Prod
  case class PagerEvent(application: String, description: String, date: Date)

  val eventSource = Source(List(
    PagerEvent("AkkaInfra", "Infrastructure broke", new Date),
    PagerEvent("FastDataPipeline", "Illegal elements in the data pipeline", new Date),
    PagerEvent("AkkaInfra", "A service stopped responding", new Date),
    PagerEvent("SuperFrontend", "A button doesn't work", new Date)
  ))

  // External service that pages someone and returns their email
  object PagerService {
    private val engineers = List("Daniel", "John", "Lady Gaga")
    private val emails = Map(
      "Daniel" -> "daniel@gmail.com",
      "John" -> "john@gmail.com",
      "Lady Gaga" -> "ladygaga@gmail.com"
    )

    def processEvent(pagerEvent: PagerEvent) = {
      println(s"Thread executing processEvents is: ${Thread.currentThread()}")
      val engineerIndex = pagerEvent.date.toInstant.getEpochSecond / (24 * 3600) % engineers.length
      val engineer = engineers(engineerIndex.toInt)
      val engineerEmail = emails(engineer)

      // page the engineer
      println(s"Sending engineer ${engineerEmail} a high priority notification: ${pagerEvent}")
      Future {
        Thread.sleep(1000)
        println(s"Thread executing processEvent future is: ${Thread.currentThread()}")
        engineerEmail
      }
    }
  }

//  implicit val dispatcher = system.dispatchers.lookup("dedicated-dispatcher")
  // Interested in events of the type "AkkaInfra"
  val infraEvents: Source[PagerEvent, NotUsed] = eventSource.filter(_.application == "AkkaInfra")
  // parallelism number of futures can run together
  val pagedEngineerEmails = infraEvents.mapAsync(parallelism = 1){event =>
    println(s"Thread executing mapAsync function: ${Thread.currentThread()}")
    PagerService.processEvent(event).map { email =>
      println(s"Thread processing email result: ${email} :: ${Thread.currentThread()}")
      email
    }
  }
  // mapAsync - guarantee the relative order of elements... waits for current future to complete
  /*
    so if, some future takes longer, entire stream is affected
    If order is of no concern, use `mapAsyncUnordered`
   */
  val pagedEmailsSink = Sink.foreach[String](email => println(s"Successfully sent notification to: ${email}"))
   pagedEngineerEmails.to(pagedEmailsSink).run()

  // Use `mapAsync` when working with Futures

  class PagerActor extends Actor with ActorLogging {
    private val engineers = List("Daniel", "John", "Lady Gaga")
    private val emails = Map(
      "Daniel" -> "daniel@gmail.com",
      "John" -> "john@gmail.com",
      "Lady Gaga" -> "ladygaga@gmail.com"
    )

    private def processEvent(pagerEvent: PagerEvent) = {
      val engineerIndex = pagerEvent.date.toInstant.getEpochSecond / (24 * 3600) % engineers.length
      val engineer = engineers(engineerIndex.toInt)
      val engineerEmail = emails(engineer)

      // page the engineer
      log.info(s"Sending engineer ${engineerEmail} a high priority notification: ${pagerEvent}")
      Thread.sleep(1000)
      engineerEmail
    }

    override def receive: Receive = {
      case pagerEvent: PagerEvent =>
        sender() ! processEvent(pagerEvent)
    }
  }

  import akka.pattern.ask
  import scala.concurrent.duration._
  implicit val timeout = Timeout(3 seconds)
  val pagerActor = system.actorOf(Props[PagerActor], "pagerActor")
  val alternativePagedEngineerEmails = infraEvents.mapAsync(parallelism = 4)(event => (pagerActor ? event).mapTo[String])
//  alternativePagedEngineerEmails.to(pagedEmailsSink).run()

  // do not confuse mapAsync with async(ASYNC boundary)
}
