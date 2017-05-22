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


object KmerMapReads extends LazyLogging {

  case class Config(reads_input: String = "", kmer_input: String = "", output: String = "", pattern: String = "",
                    n_iteration: Int = 1, k: Int = -1, min_kmer_count: Int = 2, sleep: Int = 0,
                    max_kmer_count: Int = 200, format: String = "seq",
                    scratch_dir: String = "/tmp", n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("KmerMapReads") {
      head("KmerMapReads", Utils.VERSION)

      opt[String]("reads").required().valueName("<dir/file>").action((x, c) =>
        c.copy(reads_input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")
      opt[String]("kmer").required().valueName("<file>").action((x, c) =>
        c.copy(kmer_input = x)).text("Kmers to be keep. e.g. output from kmer counting ")

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

      opt[Int](  "n_iteration").action((x, c) =>
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

  private def process_iteration(i: Int, readsRDD: RDD[(Long, String)], kmers: RDD[DNASeq], config: Config, sc: SparkContext) = {
    val kmersB = sc.broadcast(kmers.filter(s => Utils.pos_mod(s.hashCode, config.n_iteration) == i).collect.toSet)
    logger.info("iteration %d, broadcaset %d kmers".format(i, kmersB.value.size))

    val kmersRDD = readsRDD.map {
      case (id, seq) =>
        Kmer.generate_kmer(seq = seq, k = config.k).filter(kmersB.value.contains(_)).distinct.map(s => (s, List(id)))
    }.flatMap(x => x).reduceByKey(_ ++ _)

    // subsampling very abundant k-mers that appear in many reads
    // remove very rare k-mers that appear only in one read: likely due to sequencing error
    val filteredKmersRDD = kmersRDD.filter {
      x => x._2.length >= config.min_kmer_count
    }.map {
      x => if (x._2.length <= config.max_kmer_count) x else (x._1, Random.shuffle(x._2).take(config.max_kmer_count))
    }.collect

    kmersB.destroy()

    val filename = "%s/KmerMapping_%d_%s.ser".format(config.scratch_dir, i, UUID.randomUUID.toString)

    Utils.serialize_object(filename, filteredKmersRDD)
    logger.info(s"Iteration $i ,#records=${filteredKmersRDD.length} save results to $filename")

    filename
  }

  def make_reads_rdd(file: String, format: String, n_partition: Int, sc: SparkContext): RDD[(Long, String)] = {
    make_reads_rdd(file, format, n_partition, -1, sc)
  }

  def make_reads_rdd(file: String, format: String, n_partition: Int, sample_fraction: Double, sc: SparkContext): RDD[(Long, String)] = {
    if (format.equals("parquet")) throw new NotImplementedError
    else {
      var textRDD = if (n_partition > 0)
        sc.textFile(file, minPartitions = n_partition)
      else
        sc.textFile(file)
      if (sample_fraction > 0) textRDD = textRDD.sample(false, sample_fraction, Random.nextLong())
      if (format.equals("seq")) {

        textRDD.map {
          line => line.split("\t|\n")
        }.map { x => (x(0).toLong, x(2)) }
      }

      else if (format.equals("base64")) {

        textRDD.map {
          line =>
            val a = line.split(",")
            val id = a(0).toInt
            val seq = a.drop(1).map {
              t =>
                val lst = t.split(" ")
                val len = lst(0).toInt
                DNASeq.from_base64(lst(1)).to_bases(len)
            }.mkString("N")
            (id, seq)
        }

      }
      else
        throw new IllegalArgumentException
    }

  }

  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")

    val seqFiles = Utils.get_files(config.reads_input.trim(), config.pattern.trim())
    logger.debug(seqFiles)

    val readsRDD = make_reads_rdd(seqFiles, config.format, config.n_partition, sc)
    readsRDD.cache()

    val kmers =
      sc.textFile(config.kmer_input).map(_.split(" ").head.trim()).map(DNASeq.from_base64)
    kmers.cache()
    println("loaded %d kmers".format(kmers.count))
    kmers.take(5).foreach(println)


    val filenames = 0.until(config.n_iteration).map {
      i =>
        process_iteration(i, readsRDD, kmers, config, sc)
    }

    kmers.unpersist()
    readsRDD.unpersist()

    val result = filenames.map(Utils.unserialize_object).flatMap(_.asInstanceOf[Array[(DNASeq, List[Long])]]).groupBy(_._1).map(x => (x._1, x._2.flatMap(_._2))).toList

    Utils.write_textfile(config.output, result.map(x => x._1.to_base64 + " " + x._2.mkString(",")).sorted)

    //clean up
    filenames.foreach {
      fname =>
        Files.deleteIfExists(Paths.get(fname))
    }

    val totalTime1 = System.currentTimeMillis
    logger.info("Total process time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }


  def main(args: Array[String]) {

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

        logger.info(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark Kmer Map Reads")
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
