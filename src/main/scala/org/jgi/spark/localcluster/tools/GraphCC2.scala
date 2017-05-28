/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.{DNASeq, JGraph, Utils}
import sext._


object GraphCC2  extends App with  LazyLogging {

  case class Config(edge_file: String = "", output: String = "",
                    n_iteration: Int = 1, min_reads_per_cluster: Int = 10, sleep: Int = 0,
                    scratch_dir: String = "/tmp")

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


      opt[Int]("n_iteration").action((x, c) =>
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


  private def get_index(x: Int, groups: Array[(Int, Int)]): Int = {
    groups.indices.foreach {
      i =>
        val group = groups(i)
        if (x >= group._1 && x < group._2) return i
    }
    return 0
  }

  private def process_iterations(groups: Array[(Int, Int)], edges: RDD[Array[Long]], config: Config, sc: SparkContext) = {
    val n = config.n_iteration


    val vertexTuple = edges.map {
      x =>
        if (x(0) < x(1)) (x(0), x(1)) else (x(1), x(0))
    }.map {
      x =>
        val a = Utils.pos_mod(x._1.toInt, n)
        (get_index(a, groups), x)
    }

    var vetexGroupRDD = vertexTuple.groupByKey().repartition(groups.length)

    vetexGroupRDD.map {
      x =>
        val group = x._1
        val g = new JGraph(x._2)
        val clusters = g.cc
        clusters
    }
  }


  def merge_cc(clusters: RDD[(Long, Long)] /*(v,c)*/
               , raw_edges: RDD[Array[Long]], config: Config, sc: SparkContext): RDD[(Long, Long)] = {
    val edges = raw_edges.map {
      x => (x(0), x(1)) //(v,v)
    }
    val new_edges = edges.join(clusters).map(x => x._2).join(clusters).map(x => x._2).distinct //(c,c)
    val graph = new JGraph(new_edges.collect)

    val cc = sc.parallelize(graph.cc)
    val new_clusters = cc.map(x => (x._1.toLong, x._2.toLong)). //(c,x)
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
    }.toArray
    logger.info(s"request ${config.n_iteration} iterations. truly get $vertex_groups groups")

    val edges =
      sc.textFile(config.edge_file).
        map { line =>
          line.split(",").take(2).map(_.toLong)
        }

    edges.cache()
    println("loaded %d edges".format(edges.count))
    edges.take(5).map(_.mkString(",")).foreach(println)

    val clusters_list = process_iterations(vertex_groups, edges, config, sc)


    val final_clusters = if (clusters_list.count > 1) {
      val clusters = clusters_list.flatMap(x => x)
        .groupByKey.map(x => (x._1, x._2.min))

      merge_cc(clusters, edges, config, sc)
    }
    else {
      clusters_list.flatMap(x => x).map(_.swap)
    }

    val result = final_clusters.groupByKey.filter(_._2.size >= config.min_reads_per_cluster).map(_._2.toList.sorted)
      .map(_.mkString(",")).collect

    edges.unpersist()

    Utils.write_textfile(config.output, result.sorted)
    logger.info(s"total #records=${result.length} save results to ${config.output}")

    //clean up
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
