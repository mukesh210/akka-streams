package part5_advanced

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.util.{Failure, Success}

object Substreams extends App {

  implicit val system = ActorSystem("Substreams")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  // 1 - grouping a stream by a certain function
  val wordsSource = Source(List("Akka", "is", "amazing", "learning", "substreams"))
  // groupBy will return stream containing sub-streams.
  val groups = wordsSource.groupBy(30, word => if(word.isEmpty) '\0' else word.toLowerCase().charAt(0))

  // once we attach sink to above stream containing substreams, this sink will be attached to each substream separately.
  groups.to(Sink.fold(0)((count, word) => {
    val newCount = count + 1
    println(s"I just received: ${word}, count: ${newCount}")
    newCount
  }))
//    .run()
  /*
   When we attach consumer to subFlows, every single subFlow/substream will have
   different materialization of consumer
   i.e. in above example, every substream will have different instances of sink
   */

  // 2 - how to merge substreams back - for tasks that look like async mapReduce
  val textSource = Source(List(
    "I love Akka Streams",
    "this is amazing",
    "learning from Rock the JVM"
  ))

  val totalCharCountFuture = textSource.groupBy(2, string => string.length % 2)
    .map(_.length)  // do your expensive computation here, will run async on each substream
    .mergeSubstreamsWithParallelism(2)
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  totalCharCountFuture onComplete {
    case Success(value) =>  println(s"Total char count: ${value}")

    case Failure(exception) => println(s"Char computation failed: ${exception}")
  }

  // 3 - splitting a stream into substream, when a condition is met
  val text = "I love Akka Streams\n" +
  "this is amazing\n" +
  "learning from Rock the JVM\n"

  val anotherCharCountFuture = Source(text.toList)
    .splitWhen(c => c == '\n')  // 3 substreams will be created
    .filter(_ != '\n')
    .map(_ => 1)
    .mergeSubstreams
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  anotherCharCountFuture onComplete {
    case Success(value) =>  println(s"Total char count alternative: ${value}")

    case Failure(exception) => println(s"Char computation failed: ${exception}")
  }

  //  4 - Flattening
  val simpleSource = Source(1 to 5)
  simpleSource.flatMapConcat(x => Source(x to 3 * x)).runWith(Sink.foreach(println))

//   flatMapMerge will merge with parallelism = 2, at a time, 2 substream will be merged in no defined order
  simpleSource.flatMapMerge(2, x => Source(x to 3 * x)).runWith(Sink.foreach(println))
}
