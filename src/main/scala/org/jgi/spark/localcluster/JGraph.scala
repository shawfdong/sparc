package org.jgi.spark.localcluster

import com.typesafe.scalalogging.LazyLogging
import net.jligra.uintVector

/**
  * Created by Lizhen Shi on 5/27/17.
  */

class JGraph(val edges: Iterable[(Long, Long)],val n_thread:Int) extends LazyLogging {
  net.jligra.Info.load_native()

  var node_mapping: Map[Long, Int] = edges.map(x => Array(x._1, x._2)).flatten.toList.distinct.sorted.zipWithIndex.toMap //(rawv,v)
  val n_nodes: Long = node_mapping.size
  val _inverse_mapping: Map[Int, Long] = node_mapping.map(_.swap) //(v.rawv)

  def this(edges: Iterable[(Long, Long)]) = this(edges,-1)

  def invmap(i: Long): Long = _inverse_mapping(i.toInt)

  def cc: Seq[(Long, Long)] ={
    val coo_row = new uintVector()
    val coo_col = new uintVector()
    edges.foreach {
      x =>
        coo_row.add(node_mapping(x._1))
        coo_col.add(node_mapping(x._2))

        coo_row.add(node_mapping(x._2))
        coo_col.add(node_mapping(x._1))
    }
    val n_edges = coo_row.size.toInt

    val original_threads= net.jligra.ligra.getWorkers
    if (n_thread>0){
    net.jligra.ligra.setWorkers(n_thread)
    logger.info(s"OPENMP change #threads from $original_threads to $n_thread");
    } else {
      logger.info(s"OPENMP uses $original_threads threads")
    }

    val t1 = System.currentTimeMillis/1000.0
    //6, coo_row.size(), coo_row, coo_col,
    val graph = net.jligra.ligra.create_asymmetric_graph_from_coo(n_nodes, n_edges, coo_row, coo_col, System.getProperty("java.io.tmpdir"))
    val t2 = System.currentTimeMillis/1000.0
    logger.info(f"creating graph takes ${t2-t1}%.4f seconds")

    val cc = net.jligra.ligra.connected_components(graph)
    val t3 = System.currentTimeMillis/1000.0
    logger.info(f"cc takes ${t3-t2}%.4f seconds")

    net.jligra.ligra.setWorkers(original_threads)

    val node_map = (0 until n_nodes.toInt).map(x => (x, cc.get(x))).toMap

    val clusters = node_mapping.map(x => (x._1, node_map(x._2))).map(x => (x._1, invmap(x._2))).toSeq //(rawv, rawc)
    val t4 = System.currentTimeMillis/1000.0
    logger.info(f"edge mapping takes ${t4-t3}%.4f seconds")

    clusters
  }
}

object JGraph {

}