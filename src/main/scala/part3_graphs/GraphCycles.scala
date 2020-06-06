package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy, SourceShape, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, MergePreferred, RunnableGraph, Sink, Source, Zip, ZipWith}

object GraphCycles extends App {

  implicit val system = ActorSystem("GraphCycles")
  implicit val materializer = ActorMaterializer()

  val accelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(Merge[Int](2))
    val incrementerShape = builder.add(Flow[Int].map{ x =>
      println(s"Accelerating ${x}")
      x + 1
    })

    sourceShape ~> mergeShape ~> incrementerShape
                   mergeShape <~ incrementerShape

    ClosedShape
  }

  /*
  Components starts buffering element, and when buffers are full,
  they start backpressuring each other until there is no one to process any more element
  in graph
   */
  // RunnableGraph.fromGraph(accelerator).run()
  // graph cycle deadlock!

  /*
  Possible solutions for graph cycle deadlocks...
  Solution 1: MergePreferred
   */
  val actualAccelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(MergePreferred[Int](1))
    val incrementerShape = builder.add(Flow[Int].map{ x =>
      println(s"Accelerating ${x}")
      x + 1
    })

    /*
    initially, there is no input on `preferred` port of `mergeShape`, so it will consume
     an element from `SourceShape`. As soon as we have an element from sourceShape,
     'incrementerShape' will process it. Now, mergeShape will give priority to input
     arriving on it's 'preferred' port
     */
    sourceShape ~> mergeShape ~> incrementerShape
    mergeShape.preferred <~ incrementerShape

    ClosedShape
  }
  // RunnableGraph.fromGraph(actualAccelerator).run()

  /*
  Solution 2:
   */
  val bufferedRepeater = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(Merge[Int](2))
    val repeaterShape = builder.add(Flow[Int].buffer(10, OverflowStrategy.dropHead).map{ x =>
      println(s"Accelerating ${x}")
      Thread.sleep(100)
      x
    })

    sourceShape ~> mergeShape ~> repeaterShape
    mergeShape <~ repeaterShape

    ClosedShape
  }
  // RunnableGraph.fromGraph(bufferedRepeater).run()
  /*
    cycles risk deadlocking
      - add bounds to the number of elements in the cycle


      boundedness vs liveness
   */
  /**
    * Challenge: create a fan-in shape
    * - takes 2 inputs which will be fed with exactly one number (1 and 1)
    * - output will emit an infinite fibonacci sequence based off those 2 numbers
    * 1, 2, 3, 5, 8, ...
    *
    * Hint: Use ZipWith and cycles, MergePreferred
    */


  val singleSource1: Source[BigInt, NotUsed] = Source.single(1)
  val singleSource2: Source[BigInt, NotUsed] = Source.single(1)

  // my solution
  val fibonacciRunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val mergeShape = builder.add(MergePreferred[BigInt](1))
      val mergeShapeForZipWith = builder.add(MergePreferred[BigInt](1))
      val zipWith = builder.add(ZipWith[BigInt, BigInt, BigInt]((x, y) => {
        println(x+y)
        x + y
      }))
      val broadcast = builder.add(Broadcast[BigInt](2))
      val singleSource1Shape: SourceShape[BigInt] = builder.add(singleSource1)
      val singleSource2Shape: SourceShape[BigInt] = builder.add(singleSource2)

      singleSource1Shape ~> mergeShape
      singleSource2Shape ~> mergeShapeForZipWith ~> zipWith.in0

      mergeShape ~> broadcast

      broadcast.out(0) ~> zipWith.in1
      broadcast.out(1) ~> mergeShapeForZipWith.preferred

      zipWith.out ~> mergeShape.preferred

      ClosedShape
    }
  )

  println("Results are:")
  //fibonacciRunnableGraph.run()

  // tutorial's solution
  val fibonacciGenerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val zip = builder.add(Zip[BigInt, BigInt])
    val mergePreferred = builder.add(MergePreferred[(BigInt, BigInt)](1))
    val fiboLogic = builder.add(Flow[(BigInt, BigInt)].map { pair =>
      val last = pair._1
      val previous = pair._2

      Thread.sleep(100)
      (last + previous, last)
    })
    val broadcast = builder.add(Broadcast[(BigInt, BigInt)](2))
    val extractLast = builder.add(Flow[(BigInt, BigInt)].map(_._1))

    zip.out ~> mergePreferred ~> fiboLogic ~> broadcast ~> extractLast
               mergePreferred.preferred <~ broadcast

    UniformFanInShape(extractLast.out, zip.in0, zip.in1)
  }

  val fiboGraph = RunnableGraph.fromGraph(
    GraphDSL.create() {  implicit builder =>
      import GraphDSL.Implicits._

      val source1 = builder.add(Source.single[BigInt](1))
      val source2 = builder.add(Source.single[BigInt](1))
      val sink = builder.add(Sink.foreach[BigInt](println))
      val fibo = builder.add(fibonacciGenerator)

      source1 ~> fibo.in(0)
      source2 ~> fibo.in(1)
      fibo.out ~> sink

      ClosedShape
    }
  )

  fiboGraph.run()


}
