/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.Utils
import sext._

object GraphGen extends App with LazyLogging {

  case class Config(kmer_reads: String = "", output: String = "",
                    n_iteration: Int = 1, min_shared_kmers: Int = 2, max_shared_kmers: Int = 20000, sleep: Int = 0,
                    scratch_dir: String = "/tmp", n_partition: Int = 0,
                    use_bloom_filter: Boolean = false)


  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphGen") {
      head("GraphGen", Utils.VERSION)

      opt[String]('i', "kmer_reads").required().valueName("<file>").action((x, c) =>
        c.copy(kmer_reads = x)).text("reads that a kmer shares. e.g. output from KmerMapReads")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("min_shared_kmers should be greater than 1"))
        .text("minimum number of kmers that two reads share")

      opt[Int]("max_shared_kmers").action((x, c) =>
        c.copy(max_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("max_shared_kmers should be greater than 1"))
        .text("max number of kmers that two reads share")


      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")

      opt[Unit]("use_bloom_filter").action((x, c) =>
        c.copy(use_bloom_filter = true))
        .text("use bloomer filter. only applied to redis")


      opt[String]("scratch_dir").valueName("<dir>").action((x, c) =>
        c.copy(scratch_dir = x)).text("where the intermediate results are")


      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  private def process_iteration_spark(i: Int, kmer_reads: RDD[Array[Int]], config: Config, sc: SparkContext): RDD[(Int, Int, Int)] = {
    val edges = kmer_reads.map(_.combinations(2).map(x => x.sorted).filter {
      case a =>
        Utils.pos_mod((a(0) + a(1)).toInt, config.n_iteration) == i
    }.map(x => (x(0), x(1)))).flatMap(x => x)

    val rdd = edges.map(x => (x, 1)).reduceByKey(_ + _).filter(x => x._2 >= config.min_shared_kmers)
      .map(x => (x._1._1.toInt, x._1._2.toInt, x._2.toInt))

    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd

  }


  private def process_iteration(i: Int, kmer_reads: RDD[Array[Int]], config: Config, sc: SparkContext): RDD[(Int, Int, Int)] = {
    process_iteration_spark(i, kmer_reads, config, sc)
  }


  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")


    val kmer_reads =
      (if (config.n_partition > 0)
        sc.textFile(config.kmer_reads, minPartitions = config.n_partition)
      else
        sc.textFile(config.kmer_reads)
        ).
        map { line =>
          line.split(" ").apply(1).split(",").map(_.toInt)
        }

    kmer_reads.cache()
    logger.info("loaded %d kmer-reads-mapping".format(kmer_reads.count))
    kmer_reads.take(5).map(_.mkString(",")).foreach(x => logger.info(x))

    val values = 0.until(config.n_iteration).map {
      i =>
        val r = process_iteration(i, kmer_reads, config, sc)
        r
    }


    if (true) {
      //hdfs
      val rdds = values
      KmerCounting.delete_hdfs_file(config.output)
      val rdd = sc.union(rdds).map(x => s"${x._1},${x._2},${x._3}")
      rdd.saveAsTextFile(config.output)
      logger.info(s"total #records=${rdd.count} save results to hdfs ${config.output}")

      //cleanup
      try {
        kmer_reads.unpersist()
        rdds.foreach(_.unpersist())
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }


    val totalTime1 = System.currentTimeMillis
    logger.info("EdgeGen time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")
        require(config.min_shared_kmers <= config.max_shared_kmers)
        val conf = new SparkConf().setAppName("Spark Graph Gen")
        //conf.registerKryoClasses(Array(classOf[DNASeq])) //don't know why kryo cannot find the class

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
