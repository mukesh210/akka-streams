package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success}
import scala.concurrent.duration._

object CustomOperators extends App {

  implicit val system = ActorSystem("CustomOperators")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  // 1 - a custom Source which emits random numbers until cancelled

  class RandomNumberGenerator(max: Int) extends GraphStage[/*step 1: define the shape */SourceShape[Int]] {

    // step 2: define the ports and the component-specific members
    val outPort = Outlet[Int]("randomGenerator")
    val random = new Random()

    // step 3: construct a new shape
    override def shape: SourceShape[Int] = SourceShape(outPort)

    // step 4: create the logic
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // step 5: define mutable state and implementing handler on outPort
      // implement logic here

      setHandler(outPort, new OutHandler {
        // called when there is demand from downstream
        override def onPull(): Unit = {
          // emits a new element
          val nextNumber = random.nextInt(max)
          // push it out of the outPort
          push(outPort, nextNumber)
        }
      })

    }
  }

  val randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))
  // randomGeneratorSource.runWith(Sink.foreach(println))

  // 2 - custom sink that prints numbers in batches of given size

  class Batcher(batchSize: Int) extends GraphStage[SinkShape[Int]] {

    val inPort = Inlet[Int]("batcher")

    override def shape: SinkShape[Int] = SinkShape[Int](inPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      // needs this so that consumer pulls element from producer
      override def preStart(): Unit = {
        pull(inPort)
      }

      // mutable state
      val batch = new mutable.Queue[Int]

      setHandler(inPort, new InHandler {
        // called when upstream wants to send an element
        override def onPush(): Unit = {
          val nextElement = grab(inPort)
          batch.enqueue(nextElement)

          // assume some complex computation
          Thread.sleep(100)

          if(batch.size >= batchSize) {
            println("New batch: " + batch.dequeueAll(_ => true).mkString("[","," ,"]"))
          }

          pull(inPort)  // send demand upstream
        }

        override def onUpstreamFinish(): Unit = {
          if(batch.nonEmpty) {
            println("New batch: " + batch.dequeueAll(_ => true).mkString("[","," ,"]"))
            println("Stream finished.")
          }
        }
      })
    }
  }

  val batcherSink = Sink.fromGraph(new Batcher(10))
  // randomGeneratorSource.to(batcherSink).run()

  /**
    * Exercise: a custom flow - a simple filter flow
    * - 2 poers, an input and an out port
    */

  class FilterFlow[T](predicate: T => Boolean) extends GraphStage[FlowShape[T, T]] {
    val inPort = Inlet[T]("FilterInlet")
    val outPort = Outlet[T]("FilterOutlet")

    override def shape: FlowShape[T, T] = FlowShape[T, T](inPort, outPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(inPort, new InHandler {
        override def onPush(): Unit = {
          try {
            val nextNumber: T = grab(inPort)

            if(predicate(nextNumber)) {
              push(outPort, nextNumber) // pass it on
            } else {
              pull(inPort)  // ask for another element
            }
          } catch {
            case e: Throwable => failStage(e)
          }
        }
      })

      setHandler(outPort, new OutHandler {
        override def onPull(): Unit = pull(inPort)
      })
    }
  }

  val myFilter = Flow.fromGraph(new FilterFlow[Int](_ > 50))
  // randomGeneratorSource.via(myFilter).to(batcherSink).run()

  /**
    * Materialized values in graph stages
    */
  // 3 - a flow that counts the number of elements that go through it
  class CounterFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Int]] {
    val inPort = Inlet[T]("counterIn")
    val outPort = Outlet[T]("counterOut")

    override val shape: FlowShape[T, T] = FlowShape[T, T](inPort, outPort)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Int]) = {
      val promise = Promise[Int]
      val logic = new GraphStageLogic(shape) {
        // setting mutable state
        var counter = 0

        setHandler(outPort, new OutHandler {
          override def onPull(): Unit = pull(inPort)

          override def onDownstreamFinish(): Unit = {
            promise.success(counter)
            super.onDownstreamFinish()
          }
        })

        setHandler(inPort, new InHandler {
          override def onPush(): Unit = {
            // extract the element
            val nextElement = grab(inPort)
            counter += 1
            // pass it on
            push(outPort, nextElement)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(counter)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }
        })

      }

      (logic, promise.future)
    }
  }

  val counterFlow = Flow.fromGraph(new CounterFlow[Int])
  val materializedValue = Source(1 to 10)
    //.map(x => if(x == 7) throw new RuntimeException("Gotcha") else x)
    .viaMat(counterFlow)(Keep.right)
    //.to(Sink.foreach(x => if(x == 7) throw new RuntimeException("Gotcha") else x))
    .to(Sink.foreach(println))
    .run()

  materializedValue onComplete {
    case Success(value) => println(s"No of elements that passed through filter is: ${value}")
    case Failure(exception) => println(s"No of elements could not be calculated due to ${exception}")
  }
}

