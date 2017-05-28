/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.{DNASeq, Utils}
import sext._


object GraphCC  extends App with  LazyLogging {

  case class Config(edge_file: String = "", output: String = "",
                    n_iteration: Int = 1, min_reads_per_cluster: Int = 10,sleep: Int = 0,
                    scratch_dir: String = "/tmp", n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphCC") {
      head("GraphCC", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")


      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]( "n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")

      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x))
        .text("minimum reads per cluster")

      opt[String]("scratch_dir").valueName("<dir>").action((x, c) =>
        c.copy(scratch_dir = x)).text("where the intermediate results are")


      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }


  private def process_iteration(i: Int, group: (Int, Int), edges: RDD[Array[Long]], config: Config) = {
    val n = config.n_iteration
    val vertexTuple = edges.map {
      x =>
        if (x(0) < x(1)) (x(0), x(1)) else (x(1), x(0))
    }.filter { x =>
      val a = Utils.pos_mod(x._1.toInt, n)
      a >= group._1 && a < group._2
    }

    logger.info(s"group $group loaded ${vertexTuple.count} edges")


    val graph = Graph.fromEdgeTuples(
      vertexTuple, 1
    )
    val cc = graph.connectedComponents()
    val clusters = cc.vertices.map(x => (x._1.toLong, x._2.toLong))


    logger.info(s"Iteration $i ,#records=${clusters.count} are persisted")


    clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    clusters
  }


  def merge_cc(clusters: RDD[(Long, Long)] /*(v,c)*/
               , raw_edges: RDD[Array[Long]], config: Config): RDD[(Long, Long)] = {
    val edges = raw_edges.map {
      x => (x(0), x(1)) //(v,v)
    }
    val new_edges = edges.join(clusters).map(x => x._2).join(clusters).map(x => x._2) //(c,c)

    val graph = Graph.fromEdgeTuples(new_edges, 1)
    val cc = graph.connectedComponents()
    val new_clusters = cc.vertices.map(x => (x._1.toLong, x._2.toLong)). //(c,x)
      join(clusters.map(_.swap)).map(_._2) //(x,v)

    new_clusters
  }

  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")

    val vertex_groups = {
      val n = config.n_iteration
      val a = (0 to n).map(x => math.sqrt(x.toDouble / n))
        .map(x => (x * n).toInt)
      (a.take(a.length - 1), a.tail).zipped.toSet.toList.filter(x => x._1 < x._2).sorted.reverse
    }
    logger.info(s"request ${config.n_iteration} iterations. truly get $vertex_groups groups")

    val edges =
      sc.textFile(config.edge_file).
        map { line =>
          line.split(",").take(2).map(_.toLong)
        }

    edges.cache()
    println("loaded %d edges".format(edges.count))
    edges.take(5).map(_.mkString(",")).foreach(println)

    val clusters_list = vertex_groups.indices.map {
      i =>
        process_iteration(i, vertex_groups(i), edges, config)
    }


    val final_clusters = if (clusters_list.length > 1) {
      val clusters = sc.union(clusters_list)
        .groupByKey.map(x => (x._1, x._2.min))

      merge_cc(clusters, edges, config)
    }
    else {
      clusters_list(0).map(_.swap)
    }

    val result = final_clusters.groupByKey.filter(_._2.size >= config.min_reads_per_cluster).map(_._2.toList.sorted)
      .map(_.mkString(",")).collect

    edges.unpersist()

    Utils.write_textfile(config.output, result.sorted)
    logger.info(s"total #records=${result.length} save results to ${config.output}")

    //clean up
    clusters_list.foreach {
      _.unpersist()
    }

    val totalTime1 = System.currentTimeMillis
    logger.info("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark Graph CC")
        conf.registerKryoClasses(Array(classOf[DNASeq]))

        val sc = new SparkContext(conf)
        run(config, sc)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        sc.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}
