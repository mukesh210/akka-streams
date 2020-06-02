package part3_graphs

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, SinkShape, UniformFanOutShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source}

import scala.concurrent.Future
import scala.util.{Failure, Success}

// how to expose Materialized value from complex graph
object GraphMaterializedValues extends App {

  implicit val system = ActorSystem("GraphMaterializedValues")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val wordSource = Source(List("Akka", "is", "awesome", "rock", "the", "jvm"))
  val printer: Sink[String, Future[Done]] = Sink.foreach[String](println)
  val counter: Sink[String, Future[Int]] = Sink.fold[Int, String](0)((acc, _) => acc + 1)

  /*
  A composite component (sink)
  - prints out all strings which are lowercase
  - COUNTS the strings that are short (< 5 chars)
   */

  // with single materialized value
  // step 1
  val complexWordSink: Sink[String, Future[Int]] = Sink.fromGraph(
    GraphDSL.create(counter) { implicit builder => counterShape =>
      import GraphDSL.Implicits._

      // step 2: define auxiliary components
      val broadcast = builder.add(Broadcast[String](2))
      val lowerCaseFilter = builder.add(Flow[String].filter(x => x.toLowerCase == x))
      val shortWordFilter = builder.add(Flow[String].filter(_.length < 5))

      // step 3: tying the components together
      broadcast.out(0) ~> lowerCaseFilter ~> printer
      broadcast.out(1) ~> shortWordFilter ~> counterShape

      SinkShape(broadcast.in)
    }
  )
  /**
    * Steps followed for returning Materialized value from complex graph
    *
    * 1. provide component(whose materialized value you want as result of this one) in first parameter of GraphDSL.create()
    * 2. HOF which takes componentShape
    * 3. Use componentShape through out entire implementation
    */

  // with double materialized value
  val complexWordSink1 = Sink.fromGraph(
    GraphDSL.create(printer, counter)((printerMatValue, counterMatValue) => counterMatValue) { implicit builder => (printerShape, counterShape) =>
      import GraphDSL.Implicits._

      // step 2: define auxiliary components
      val broadcast = builder.add(Broadcast[String](2))
      val lowerCaseFilter = builder.add(Flow[String].filter(x => x.toLowerCase == x))
      val shortWordFilter = builder.add(Flow[String].filter(_.length < 5))

      // step 3: tying the components together
      broadcast.out(0) ~> lowerCaseFilter ~> printerShape
      broadcast.out(1) ~> shortWordFilter ~> counterShape

      SinkShape(broadcast.in)
    }
  )

  val shortStringsCountFuture = wordSource.toMat(complexWordSink1)(Keep.right).run()

  shortStringsCountFuture onComplete {
    case Success(value) => println(s"The total number of short strings: ${value}")
    case Failure(exception) => println(s"The count of short string failed... ${exception}")
  }

  /**
    * Exercise
    * Future[Int]: number of elements that went through original flow
    */
  val countFlow = Flow
  def enhanceFlow[A, B](flow: Flow[A, B, _]): Flow[A, B, Future[Int]] = {
    val counterSink = Sink.fold[Int, B](0)((count, _) => count + 1)

    Flow.fromGraph(
      GraphDSL.create(counterSink) { implicit builder => counterSinkShape =>
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[B](2))
        val originalFlowShape = builder.add(flow)

        originalFlowShape ~> broadcast ~> counterSinkShape

        FlowShape(originalFlowShape.in, broadcast.out(1))
      }
    )

  }

  val simpleSource = Source(1 to 42)
  val simpleSink = Sink.ignore
  val simpleFlow = Flow[Int].map(x => x)

  val enhancedFlowCountFuture = simpleSource.viaMat(enhanceFlow(simpleFlow))(Keep.right).toMat(simpleSink)(Keep.left).run()
  enhancedFlowCountFuture onComplete {
    case Success(count) => println(s"${count} Elements went through flow")
    case Failure(ex) => println(s"Failed: ${ex}")
  }

}
