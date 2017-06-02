package org.jgi.spark.localcluster

import net.jligra.uintVector

/**
  * Created by Lizhen Shi on 5/27/17.
  */

class JGraph(val edges: Iterable[(Long, Long)]) {
  net.jligra.Info.load_native()

  var node_mapping: Map[Long, Int] = edges.map(x => Array(x._1, x._2)).flatten.toList.distinct.sorted.zipWithIndex.toMap
  val n_nodes: Long = node_mapping.size
  val _inverse_mapping: Map[Int, Long] = node_mapping.map(_.swap)

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

    //6, coo_row.size(), coo_row, coo_col,
    val graph = net.jligra.ligra.create_asymmetric_graph_from_coo(n_nodes, n_edges, coo_row, coo_col, System.getProperty("java.io.tmpdir"))
    val cc = net.jligra.ligra.connected_components(graph)
    val node_map = (0 until n_nodes.toInt).map(x => (x, cc.get(x))).toMap

    val clusters = node_mapping.map(x => (x._1, node_map(x._2))).map(x => (x._1, invmap(x._2))).toSeq
    clusters
  }
}

object JGraph {

}