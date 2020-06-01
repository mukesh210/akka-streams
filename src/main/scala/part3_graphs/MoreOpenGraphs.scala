package part3_graphs

import java.util.Date

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, FanOutShape2, Graph, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, Zip, ZipWith}

object MoreOpenGraphs extends App {

  implicit val system = ActorSystem("MoreOpenGraphs")
  implicit val materializer = ActorMaterializer()

  /*
    Example: Max3 operator
    - 3 inputs of type int
    - the maximum of the 3
   */

  // step 1
  val max3StaticGraph: Graph[UniformFanInShape[Int, Int], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    // step 2 - define auxiliary shapes
    val max1 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))
    val max2 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))

    // step 3
    max1.out ~> max2.in0

    // step 4
    // uniform because it accepts all inputs of same type(Int)
    UniformFanInShape(max2.out, max1.in0, max1.in1, max2.in1)
  }

  val source1 = Source(1 to 10)
  val source2 = Source((1 to 10).map(_ => 5))
  val source3 = Source((1 to 10).reverse)

  val maxSink = Sink.foreach[Int](x => println(s"Max is: ${x}"))

  val max3RunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2 - declare components and SHAPES
      val max3Shape: UniformFanInShape[Int, Int] = builder.add(max3StaticGraph)

      // step 3 - tie all of them up
      source1 ~> max3Shape.in(0)
      source2 ~> max3Shape.in(1)
      source3 ~> max3Shape.in(2)

      max3Shape.out ~> maxSink

      ClosedShape
    }
  )

  // max3RunnableGraph.run()

  // same for UniformFanOutShape  - all outputs will have same type

  /*
    Non-uniform fan out shape

    Processing bank transactions
    Txn suspicious if amount > 10000

    Streams components for txns
    - output1: let the transaction go through(type: transaction)
    - output2: suspicious txn ids(type: String)
   */

  case class Transaction(id: String, source: String, recipient: String, amount: Int, date: Date)

  val transactionSource = Source(List(
    Transaction("34454564", "Paul", "Jim", 100, new Date()),
    Transaction("3434545", "Daniel", "Jim", 100000, new Date()),
    Transaction("456456", "Jim", "Alice", 7000, new Date())
  ))

  val bankProcessor = Sink.foreach[Transaction](println)
  val suspiciousAnalysisService = Sink.foreach[String](txnId => println(s"Suspicious transaction id: ${txnId}"))

  val suspiciousTransactionStaticGraph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    // step 2 - define SHAPES
    val broadcast = builder.add(Broadcast[Transaction](2))
    val suspiciousTxnFilter = builder.add(Flow[Transaction].filter(_.amount > 10000))
    val txnIdExtractor = builder.add(Flow[Transaction].map(_.id))

    // step 3 - tie shapes together
    broadcast.out(0) ~> suspiciousTxnFilter ~> txnIdExtractor

    // step 4
    new FanOutShape2(broadcast.in, broadcast.out(1), txnIdExtractor.out)
  }

  val suspiciousTxnRunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2
      val suspiciousTxnShape = builder.add(suspiciousTransactionStaticGraph)

      // step 3: tie SHAPES together
      transactionSource ~> suspiciousTxnShape.in

      suspiciousTxnShape.out0 ~> bankProcessor
      suspiciousTxnShape.out1 ~> suspiciousAnalysisService

      // step 4 - return SHAPE
      ClosedShape
    }
  )

  suspiciousTxnRunnableGraph.run()



}