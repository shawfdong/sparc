package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object MyLabelPropagation {

    /**
      * Run static Label Propagation for detecting communities in networks.
      *
      * Each node in the network is initially assigned to its own community. At every superstep, nodes
      * send their community affiliation to all neighbors and update their state to the mode community
      * affiliation of incoming messages.
      *
      * LPA is a standard community detection algorithm for graphs. It is very inexpensive
      * computationally, although (1) convergence is not guaranteed and (2) one can end up with
      * trivial solutions (all nodes are identified into a single community).
      *
      * @tparam ED the edge attribute type (not used in the computation)
      *
      * @param graph the graph for which to compute the community affiliation
      * @param maxSteps the number of supersteps of LPA to be performed. Because this is a static
      * implementation, the algorithm will run for exactly this many supersteps.
      *
      * @return a graph with vertex attributes containing the label of community affiliation
      */
    def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
      require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

      val lpaGraph = graph.mapVertices { case (vid, _) => vid }
      def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Int])] = {
        Iterator((e.srcId, Map(e.dstAttr -> 1)), (e.dstId, Map(e.srcAttr -> 1)))
      }
      def mergeMessage(count1: Map[VertexId, Int], count2: Map[VertexId, Int])
      : Map[VertexId, Int] = {
        (count1.keySet ++ count2.keySet).map { i =>
          val count1Val = count1.getOrElse(i, 0)
          val count2Val = count2.getOrElse(i, 0)
          i -> (count1Val + count2Val)
        }.toMap
      }
      def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Int]): VertexId = {
        if (message.isEmpty) attr else message.maxBy(_._2)._1
      }
      val initialMessage = Map[VertexId, Int]()
      MyPregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage)
    }
  }

object MyPregel extends Logging {

  /**
    * Execute a Pregel-like iterative vertex-parallel abstraction.  The
    * user-defined vertex-program `vprog` is executed in parallel on
    * each vertex receiving any inbound messages and computing a new
    * value for the vertex.  The `sendMsg` function is then invoked on
    * all out-edges and is used to compute an optional message to the
    * destination vertex. The `mergeMsg` function is a commutative
    * associative function used to combine messages destined to the
    * same vertex.
    *
    * On the first iteration all vertices receive the `initialMsg` and
    * on subsequent iterations if a vertex does not receive a message
    * then the vertex-program is not invoked.
    *
    * This function iterates until there are no remaining messages, or
    * for `maxIterations` iterations.
    *
    * @tparam VD the vertex data type
    * @tparam ED the edge data type
    * @tparam A the Pregel message type
    *
    * @param graph the input graph.
    *
    * @param initialMsg the message each vertex will receive at the first
    * iteration
    *
    * @param maxIterations the maximum number of iterations to run for
    *
    * @param activeDirection the direction of edges incident to a vertex that received a message in
    * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
    * out-edges of vertices that received a message in the previous round will run. The default is
    * `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
    * in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
    * *both* vertices received a message.
    *
    * @param vprog the user-defined vertex program which runs on each
    * vertex and receives the inbound message and computes a new vertex
    * value.  On the first iteration the vertex program is invoked on
    * all vertices and is passed the default message.  On subsequent
    * iterations the vertex program is only invoked on those vertices
    * that receive messages.
    *
    * @param sendMsg a user supplied function that is applied to out
    * edges of vertices that received messages in the current
    * iteration
    *
    * @param mergeMsg a user supplied function that takes two incoming
    * messages of type A and merges them into a single message of type
    * A.  ''This function must be commutative and associative and
    * ideally the size of A should not increase.''
    *
    * @return the resulting graph at the end of the computation
    *
    */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[VD, ED] =
  {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).persist(StorageLevel.MEMORY_AND_DISK)
    // compute the messages
    var messages = GraphXUtils.mapReduceTriplets(g, sendMsg, mergeMsg).persist(StorageLevel.MEMORY_AND_DISK)
    var activeMessages = messages.count()
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog).persist(StorageLevel.MEMORY_AND_DISK)

      val oldMessages = messages
      // Send new messages, skipping edges where neither side received a message. We must cache
      // messages so it can be materialized on the next line, allowing us to uncache the previous
      // iteration.
      messages = GraphXUtils.mapReduceTriplets(
        g, sendMsg, mergeMsg,None).persist(StorageLevel.MEMORY_AND_DISK)
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
      activeMessages = messages.count()

      logInfo("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
    }
    messages.unpersist(blocking = false)
    g
  } // end of apply

} // end of class Pregel
