/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import java.nio.file.{Files, Paths}
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.{DNASeq, Kmer, Utils}
import sext._

import scala.util.Random


object GraphGen extends LazyLogging {

  case class Config(kmer_reads: String = "", output: String = "",
                    n_iteration: Int = 1, k: Int = -1, min_shared_kmers: Int = 2,
                    scratch_dir: String = "/tmp", n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphGen") {
      head("GraphGen", Utils.VERSION)

      opt[String]('i', "kmer_reads").required().valueName("<file>").action((x, c) =>
        c.copy(kmer_reads = x)).text("reads that a kmer shares. e.g. output from KmerMapReads")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")

      opt[Int]('k', "kmer_length").required().action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 2) success
          else failure("min_shared_kmers should be greater than 2"))
        .text("minimum number of kmers that two reads share")


      opt[Int]('n', "n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")


      opt[String]("scratch_dir").valueName("<dir>").action((x, c) =>
        c.copy(scratch_dir = x)).text("where the intermediate results are")


      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  private def process_iteration(i: Int, kmer_reads: RDD[Array[Long]], config: Config) = {
    val edges = kmer_reads.map(_.combinations(2).map(x => x.sorted).filter {
      case a =>
        Utils.pos_mod((a(0) + a(1)).toInt, config.n_iteration) == i
    }.map(x => (x(0), x(1)))).flatMap(x => x)
    val groupedEdges = edges.countByValue().filter(x => x._2 >= config.min_shared_kmers)
      .map(x => Array(x._1._1, x._1._2, x._2)).toList

    val filename = "%s/GraphGen_%d_%s.ser".format(config.scratch_dir, i, UUID.randomUUID.toString)

    Utils.serialize_object(filename, groupedEdges)
    logger.info(s"Iteration $i ,#records=${groupedEdges.length} save results to $filename")

    filename
  }


  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")


    val kmer_reads =
      sc.textFile(config.kmer_reads).
        map { line =>
          line.split(" ").apply(1).split(",").map(_.toLong)
        }

    kmer_reads.cache()
    println("loaded %d kmer-reads-mapping".format(kmer_reads.count))
    kmer_reads.take(5).map(_.mkString(",")).foreach(println)


    val filenames = 0.until(config.n_iteration).map {
      i =>
        process_iteration(i, kmer_reads, config)
    }

    kmer_reads.unpersist()

    val result = filenames.map(Utils.unserialize_object).flatMap(_.asInstanceOf[List[Array[Long]]])

    Utils.write_textfile(config.output, result.map(_.mkString(",")).sorted)
    logger.info(s"total #records=${result.length} save results to $config.output")

    //clean up
    filenames.foreach {
      fname =>
        Files.deleteIfExists(Paths.get(fname))
    }

    val totalTime1 = System.currentTimeMillis
    logger.info("kmer counting time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }


  def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark Graph Gen")
        conf.registerKryoClasses(Array(classOf[DNASeq]))

        val sc = new SparkContext(conf)
        run(config, sc)
        sc.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}
