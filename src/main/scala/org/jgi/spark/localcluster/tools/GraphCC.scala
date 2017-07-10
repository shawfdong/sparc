/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.jgi.spark.localcluster.{DNASeq, Utils}
import sext._


object GraphCC extends App with LazyLogging {


  case class Config(edge_file: String = "", output: String = "", min_shared_kmers: Int = 2,
                    n_iteration: Int = 1, min_reads_per_cluster: Int = 2, max_shared_kmers: Int = 20000, sleep: Int = 0,
                    scratch_dir: String = "/tmp", n_partition: Int = 0, use_graphframes: Boolean = false)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphCC") {
      head("GraphCC", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")

      opt[Unit]("use_graphframes").action((x, c) =>
        c.copy(use_graphframes = true))

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("min_shared_kmers should be greater than 2"))
        .text("minimum number of kmers that two reads share")

      opt[Int]("max_shared_kmers").action((x, c) =>
        c.copy(max_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("max_shared_kmers should be greater than 1"))
        .text("max number of kmers that two reads share")

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


  def cc(edgeTuples: RDD[(Long, Long)], config: Config, sqlContext: SQLContext) = {
    if (config.use_graphframes)
      cc_graphframes(edgeTuples, sqlContext)
    else
      cc_mllib(edgeTuples)

  }

  def cc_mllib(edgeTuples: RDD[(Long, Long)]) = {
    val graph = Graph.fromEdgeTuples(
      edgeTuples, 1
    )
    val cc = graph.connectedComponents()
    val clusters = cc.vertices.map(x => (x._1.toLong, x._2.toLong))
    clusters
  }

  def cc_graphframes(edgeTuples: RDD[(Long, Long)], sqlContext: SQLContext) = {
    import org.graphframes.GraphFrame
    val edges = sqlContext.createDataFrame(edgeTuples.map {
      case (src, dst) =>
        (src, dst, 1)
    }).toDF("src", "dst", "cnt")
    val graph = GraphFrame.fromEdges(edges)

    val cc = graph.connectedComponents.run()
    val clusters = cc.select("id", "component").rdd.map(x => (x(0).asInstanceOf[Long], x(1).asInstanceOf[Long]))
    clusters
  }

  private def process_iteration(i: Int, group: (Int, Int), edges: RDD[Array[Long]], config: Config, sqlContext: SQLContext) = {
    val n = config.n_iteration
    val edgeTuples = edges.map {
      x =>
        if (x(0) < x(1)) (x(0), x(1)) else (x(1), x(0))
    }.filter { x =>
      val a = Utils.pos_mod(x._1.toInt, n)
      a >= group._1 && a < group._2
    }

    logger.info(s"group $group loaded ${edgeTuples.count} edges")

    val clusters = this.cc(edgeTuples, config, sqlContext)
    clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)

    logger.info(s"Iteration $i ,#records=${clusters.count} are persisted")

    clusters
  }


  def merge_cc(clusters: RDD[(Long, Long)] /*(v,c)*/
               , raw_edges: RDD[Array[Long]], config: Config, sqlContext: SQLContext): RDD[(Long, Long)] = {
    val edges = raw_edges.map {
      x => (x(0), x(1)) //(v,v)
    }
    val new_edges = edges.join(clusters).map(x => x._2).join(clusters).map(x => x._2) //(c,c)

    val new_clusters = cc(new_edges, config, sqlContext = sqlContext). //(c,x)
      join(clusters.map(_.swap)).map(_._2) //(x,v)

    new_clusters
  }

  def run(config: Config, spark: SparkSession): Long = {

    val sc = spark.sparkContext
    var sqlContext = spark.sqlContext

    sc.setCheckpointDir(System.getProperty("java.io.tmpdir"))

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
      (if (config.n_partition > 0)
        sc.textFile(config.edge_file).repartition(config.n_partition)
      else
        sc.textFile(config.edge_file)).
        map { line =>
          line.split(",").map(_.toLong)
        }.filter(x => x(2) >= config.min_shared_kmers && x(2) <= config.max_shared_kmers).map(_.take(2))

    edges.cache()
    logger.info("loaded %d edges".format(edges.count))
    edges.take(5).map(_.mkString(",")).foreach(println)

    val clusters_list = vertex_groups.indices.map {
      i =>
        process_iteration(i, vertex_groups(i), edges, config, sqlContext)
    }


    val final_clusters = if (clusters_list.length > 1) {
      val clusters = sc.union(clusters_list)
        .groupByKey.map(x => (x._1, x._2.min))

      merge_cc(clusters, edges, config, sqlContext)
    }
    else {
      clusters_list(0).map(_.swap)
    }

    KmerCounting.delete_hdfs_file(config.output)

    val result = final_clusters.groupByKey.filter(_._2.size >= config.min_reads_per_cluster).map(_._2.toList.sorted)
      .map(_.mkString(","))//.repartition(180 )

    result.saveAsTextFile(config.output)
    val result_count = result.count
    logger.info(s"total #records=${result_count} save results to ${config.output}")

    val totalTime1 = System.currentTimeMillis
    logger.info("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))

    // may be have the bug as https://issues.apache.org/jira/browse/SPARK-15002
    //edges.unpersist()

    //clean up
    clusters_list.foreach {
      _.unpersist()
    }
    result_count
  }


  override def main(args: Array[String]) {
    val APPNAME = "GraphCC"

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")
        require(config.min_shared_kmers <= config.max_shared_kmers)

        val conf = new SparkConf().setAppName(APPNAME)
        conf.registerKryoClasses(Array(classOf[DNASeq]))

        val spark = SparkSession
          .builder().config(conf)
          .appName(APPNAME)
          .getOrCreate()

        run(config, spark)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        spark.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}
