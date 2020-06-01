package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, FanInShape2}
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}

object GraphBasics extends App {

  implicit val system = ActorSystem("GraphBasics")
  implicit val materializer = ActorMaterializer()

  // run incrementer & multiplier with input and merge result into one
  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(_ + 1) // hard computation
  val multiplier = Flow[Int].map(_ * 10)
  val output = Sink.foreach[(Int, Int)](println)

  // step 1: setting up the fundamentals for the graph
  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>  // builder = MUTABLE data structure
      import GraphDSL.Implicits._ // brings nice operator into scope

      // step 2: add the necessary components of this graph
      val broadcast = builder.add(Broadcast[Int](2))  // fan-out operator
      val zip: FanInShape2[Int, Int, (Int, Int)] = builder.add(Zip[Int, Int])  // fan-in operator

      // step 3: tying up the components
      input ~> broadcast

      broadcast.out(0) ~> incrementer ~> zip.in0
      broadcast.out(1) ~> multiplier ~> zip.in1

      zip.out ~> output

      // step 4: return a closed shape
      ClosedShape // FREEZE the builder's shape

      // shape
    } // static graph
  ) // runnable graph

  // graph.run() // run the graph and materialize it

  /**
    * exercise 1: feed a source into 2 sinks at the same time(hint: use a broadcast)
    *
    */
  val sink1 = Sink.foreach[Int](x => println(s"Sink 1: ${x}"))
  val sink2 = Sink.foreach[Int](x => println(s"Sink 2: ${x}"))

  // step 1
  val sourceTo2SinksGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      // step 2: declaring the components
      val broadcast = builder.add(Broadcast[Int](2))  // fan-out operator

      // step 3: tying up the components
      input ~> broadcast  // read as "input feeds into broadcast"
      broadcast.out(0) ~> sink1
      broadcast.out(1) ~> sink2

      // or step 3 can be written as: (Implicit port numbering)
      /*input ~> broadcast ~> sink1
               broadcast ~> sink2*/

      // step 4
      ClosedShape

    } // Static graph
  )   // Runnable graph

  // sourceTo2SinksGraph.run()

  /**
    * exercise 2: balances
    *
    * when we don't know the rate of production of 2 different sources,
    * use merge and balance to balance out the flow
    *
    * if you run below example, we will observe that even though
    * rate of production of 2 sources are different, output is balanced out
    */
    import scala.concurrent.duration._
  val fastSource = input.throttle(5, 1 second)
  val slowSource = input.throttle(2, 1 second)
  val sink11 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 1 number of elements: ${count}")
    count + 1
  })
  val sink21 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 2 number of elements: ${count}")
    count + 1
  })

  // step 1
  val mergeBalanceGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2 -- declare components
      val merge = builder.add(Merge[Int](2))  // fan-in shape
      val balance = builder.add(Balance[Int](2)) // fan-out shape

      // step 3 - tie them up
      fastSource ~> merge ~> balance ~> sink11
      slowSource ~> merge
      balance ~> sink21

      // step 4
      ClosedShape
    }
  )

  mergeBalanceGraph.run()



}
