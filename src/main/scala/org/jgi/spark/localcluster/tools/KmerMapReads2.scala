/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster._
import sext._

import scala.util.Random


object KmerMapReads2 extends App with LazyLogging {

  case class Config(reads_input: String = "", output: String = "", pattern: String = "",
                    _contamination: Double = 0.00005, n_iteration: Int = 1, k: Int = -1, min_kmer_count: Int = 2, sleep: Int = 0,
                    max_kmer_count: Int = 200, format: String = "seq", canonical_kmer: Boolean = false,
                    scratch_dir: String = "/tmp", n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("KmerMapReads") {
      head("KmerMapReads", Utils.VERSION)

      opt[String]("reads").required().valueName("<dir/file>").action((x, c) =>
        c.copy(reads_input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")


      opt[String]('p', "pattern").valueName("<pattern>").action((x, c) =>
        c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")


      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")

      opt[String]("format").valueName("<format>").action((x, c) =>
        c.copy(format = x)).
        validate(x =>
          if (List("seq", "parquet", "base64").contains(x)) success
          else failure("only valid for seq, parquet or base64")
        ).text("input format (seq, parquet or base64)")


      opt[Unit]('C', "canonical_kmer").action((_, c) =>
        c.copy(canonical_kmer = true)).text("apply canonical kmer")

      opt[Double]('c', "contamination").action((x, c) =>
        c.copy(_contamination = x)).
        validate(x =>
          if (x > 0 && x <= 1) success
          else failure("contamination should be positive and less than 1"))
        .text("the fraction of top k-mers to keep, others are removed likely due to contamination")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input, only applicable to local files")

      opt[Int]('k', "kmer_length").required().action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Int]("min_kmer_count").action((x, c) =>
        c.copy(min_kmer_count = x)).
        validate(x =>
          if (x >= 2) success
          else failure("min_kmer_count should be greater than 2"))
        .text("minimum number of reads that shares a kmer")

      opt[Int]("max_kmer_count").action((x, c) =>
        c.copy(max_kmer_count = x)).
        validate(x =>
          if (x >= 2) success
          else failure("max_kmer_count should be greater than 2"))
        .text("maximum number of reads that shares a kmer. greater than max_kmer_count, however don't be too big")

      opt[Int]("n_iteration").action((x, c) =>
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

  def logInfo(str: String)={
    println(str)
    logger.info(str)
  }

  private def process_iteration(i: Int, readsRDD: RDD[(Long, String)],
                                config: Config, sc: SparkContext) = {


    val kmer_gen_fun = (seq: String) => if (config.canonical_kmer) Kmer.generate_kmer(seq = seq, k = config.k) else Kmer2.generate_kmer(seq = seq, k = config.k)

    val kmersRDD = readsRDD.mapPartitions {
      iterator =>
        iterator.map {
          case (id, seq) =>
            kmer_gen_fun(seq)
              .filter { x =>
                (Utils.pos_mod(x.hashCode, config.n_iteration) == i)
              }
              .distinct.map(s => (s, Set(id)))
        }
    }.flatMap(x => x).reduceByKey(_ ++ _).filter(u=>u._2.size>1)
      .sortBy(u => (-u._2.size, u._1.hashCode))
      .zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK)

    val n_kmers = kmersRDD.count
    val Nth_idx = (n_kmers * config._contamination).toLong

    logInfo(s"Generate ${n_kmers} kmers (#count>1) for iteration $i. Will drop ${Nth_idx} kmers.")
    //val Nth_size=kmersRDD.filter(u=>Nth_idx==u._2).first._1._2.size
    // subsampling very abundant k-mers that appear in many reads
    // remove very rare k-mers that appear only in one read: likely due to sequencing error
    val filteredKmersRDD = kmersRDD.filter(u => u._2 >= Nth_idx).map(u => u._1).filter {
      x => x._2.size >= config.min_kmer_count
    }.map {
      x => if (x._2.size <= config.max_kmer_count) x else (x._1, Random.shuffle(x._2).take(config.max_kmer_count))
    }

    //filteredKmersRDD.persist(StorageLevel.MEMORY_AND_DISK)
    logInfo(s"Finish Iteration $i with filtered count ${filteredKmersRDD.count}")
    //kmersRDD.unpersist(blocking = false)
    filteredKmersRDD

  }


  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    val seqFiles = Utils.get_files(config.reads_input.trim(), config.pattern.trim())
    logger.debug(seqFiles)

    val readsRDD = KmerMapReads.make_reads_rdd(seqFiles, config.format, config.n_partition, sc)
    readsRDD.cache()


    val rdds = 0.until(config.n_iteration).map {
      i =>
        process_iteration(i, readsRDD, config, sc)
    }

    KmerCounting.delete_hdfs_file(config.output)
    sc.union(rdds).map(x => x)
      //.groupBy(_._1).map(x => (x._1, x._2.flatMap(_._2)))
      .map(x => x._1.to_base64 + " " + x._2.mkString(","))
      .saveAsTextFile(config.output)

    //clean up
    rdds.foreach(_.unpersist())
    readsRDD.unpersist()

    val totalTime1 = System.currentTimeMillis
    logInfo("Total process time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get
        if (config.reads_input.startsWith("hdfs:") && config.n_partition > 0) {
          logger.error("do not set partition when use hdfs input. Change block size of hdfs instead")
          sys.exit(-1)
        }

        if (config.max_kmer_count < config.min_kmer_count) {
          logger.error("max_kmer_count should not be less than min_kmer_count")
          sys.exit(-1)
        }

        logInfo(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark Kmer Map Reads").set("spark.kryoserializer.buffer.max", "512m")
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
