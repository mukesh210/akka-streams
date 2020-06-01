package part2_primer

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * When running a graph, all components return materialized value
  * so, it is upto us to choose which materialized value we want as result of running graph
  *
  * By default: left most materialized value is kept
  */
object MaterializingStreams extends App {

  implicit val system = ActorSystem("MaterializingStreams")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val simpleGraph: RunnableGraph[NotUsed] = Source(1 to 10).via(Flow[Int].map(x => x * 2)).to(Sink.foreach(println))
  // val simpleMaterializedValue: NotUsed = simpleGraph.run()

  val source: Source[Int, NotUsed] = Source(1 to 10)
  val sink: Sink[Int, Future[Int]] = Sink.reduce[Int](_ + _)
//  val sumFuture: Future[Int] = source.runWith(sink)
//  sumFuture.onComplete {
//    case Success(value) =>
//      println(s"Sum of all elements is: ${value}")
//    case Failure(ex) => println(s"Sum of element could not be computed: ${ex}")
//  }

  /**
    * By default: Source -> flow -> flow -> ... -> Sink
    *
    * In this flow, by default left most materialized value is kept
    *
    * That's why Materialized value of below expression is `NotUsed`
    * val simpleGraph: RunnableGraph[NotUsed] = Source(1 to 10).via(Flow[Int].map(x => x * 2)).to(Sink.foreach(println))
    */

  // choosing materialized values
  val simpleSource: Source[Int, NotUsed] = Source(1 to 10)
  val simpleFlow: Flow[Int, Int, NotUsed] = Flow[Int].map(_ + 1)
  val simpleSink: Sink[Int, Future[Done]] = Sink.foreach[Int](println)
  // val x: RunnableGraph[NotUsed] = simpleSource.via(simpleFlow).to(simpleSink)
//  val graph: RunnableGraph[Future[Done]] = simpleSource.viaMat(simpleFlow)(Keep.right).toMat(simpleSink)(Keep.right)
//  graph.run().onComplete {
//    case Success(_) => println("Stream processing finished")
//    case Failure(ex) => println(s"Stream processing failed with: ${ex}")
//  }

  // sugars
  // val sum: Future[Int] = Source(1 to 10).runWith(Sink.reduce[Int](_ + _))  // source.to(Sink.reduce)(Keep.right)
  // val o2: Future[Int] = Source(1 to 10).runReduce(_ + _)  // uses runWith and Keep.right

  //  backwards
  // Sink.foreach[Int](println).runWith(Source.single(42)) // source(...).to(Sink...).run()
  // both ways
  // Flow[Int].map(2 * _).runWith(simpleSource, simpleSink)

  /**
    * - return the last element out of a source (use Sink.last)
    * - compute total word count out of a stream of sentences
    *   - map, fold, reduce
    */
  // Exercise - 1
  val source1: Source[Int, NotUsed] = Source[Int](1 to 10)
  val sink1: Sink[Int, Future[Int]] = Sink.last[Int]

  // first option
  val p1: RunnableGraph[Future[Int]] = source1.toMat(sink1)(Keep.right)
  executeOnComplete(p1.run())

  // second option
  executeOnComplete(source1.runWith(sink1))

  def executeOnComplete(x: Future[Int]) = {
    x.onComplete {
      case Success(value) => println(s"last element of source1: ${value}")
      case Failure(ex) => println(s"last element of source1 could not be computed: ${ex}")
    }
  }

  // 2nd exercise
  val sentences = List(
    "Scala is awesome",
    "Akka is awesome library",
    "Akka Streams is really cool"
  )
  val source2: Source[String, NotUsed] = Source[String](sentences)
  val sink2 = Sink.fold[Int, String](0)((acc, sentence) => acc + sentence.split(" ").length)
  // 1st
  wordCountOnComplete(source2.toMat(sink2)(Keep.right).run())
  // 2nd
  wordCountOnComplete(source2.runWith(sink2))
  // 3rd
  wordCountOnComplete(source2.runFold(0)((acc, sentence) => acc + sentence.split(" ").length))

  // 4th
  val wordCountFlow = Flow[String].fold[Int](0)((acc, sentence) => acc + sentence.split(" ").length)
  wordCountOnComplete(source2.via(wordCountFlow).toMat(Sink.last)(Keep.right).run())

  // 5th
  wordCountOnComplete(source2.viaMat(wordCountFlow)(Keep.left).toMat(Sink.head)(Keep.right).run())

  wordCountOnComplete(source2.via(wordCountFlow).runWith(Sink.head))

  wordCountOnComplete(wordCountFlow.runWith(source2, Sink.head)._2)

  def wordCountOnComplete(x: Future[Int]) = {
    x onComplete {
      case Success(value) => println(s"word count done: ${value}")
      case Failure(ex) => println(s"Word count failed: ${ex}")
    }
  }
}
