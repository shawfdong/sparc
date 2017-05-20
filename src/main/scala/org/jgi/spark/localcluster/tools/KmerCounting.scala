/**
  * Created by Lizhen Shi on 5/13/17.
  */
package org.jgi.spark.localcluster.tools

import java.nio.file.{Files, Paths}
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.{DNASeq, Kmer, Utils}
import sext._


object KmerCounting extends LazyLogging {

  case class Config(input: String = "", output: String = "", n_iteration: Int = 1, pattern: String = "",
                    _contamination: Double = 0.00005, k: Int = 31, format: String = "seq", sleep: Int = 0,
                    scratch_dir: String = "/tmp", n_partition: Int = 0, sample_fraction: Double = -1.0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("KmerCounting") {
      head("kmer counting", Utils.VERSION)

      opt[String]('i', "input").required().valueName("<dir>").action((x, c) =>
        c.copy(input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

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

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input, only applicable to local files")

      opt[Int]("sleep").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]('k', "kmer_length").action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Int]('n', "n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")


      opt[Double]('c', "contamination").action((x, c) =>
        c.copy(_contamination = x)).
        validate(x =>
          if (x > 0 && x <= 1) success
          else failure("contamination should be positive and less than 1"))
        .text("the fraction of top k-mers to keep, others are removed likely due to contamination")

      opt[Double]("sample_fraction").action((x, c) =>
        c.copy(sample_fraction = x)).
        validate(x =>
          if (x < 1) success
          else failure("should be less than 1"))
        .text("the fraction of reads to sample. if it is less than or equal to 0, no sample")

      opt[String]("scratch_dir").valueName("<dir>").action((x, c) =>
        c.copy(scratch_dir = x)).text("where the intermediate results are")


      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  def delete_hdfs_file(filepath: String): Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    val conf = new Configuration();

    val output = new Path(filepath);
    val hdfs = FileSystem.get(conf);

    // delete existing directory
    if (hdfs.exists(output)) {
      hdfs.delete(output, true);
    }
  }

  private def generate_for_iteration(i: Int, readsRDD: RDD[String], config: Config) = {
    val smallKmersRDD = readsRDD.map(x => Kmer.generate_kmer(seq = x, k = config.k)).flatMap(x => x)
      .filter(o => Utils.pos_mod(o.hashCode, config.n_iteration) == i)
      .map((_, 1)).reduceByKey(_ + _)

    val kmer_count = smallKmersRDD.count

    val topN = (kmer_count * contamination(config) * math.min(1.5, config.n_iteration)).toInt
    logger.info(s"Iteration $i , number of kmers: $kmer_count, take top $topN")

    val topKmers = smallKmersRDD.takeOrdered(topN)(Ordering[Int].reverse.on { x => x._2 })

    val filename = "%s/kmercounting_%d_%s.ser".format(config.scratch_dir, i, UUID.randomUUID.toString)

    Utils.serialize_object(filename, topKmers)
    logger.info(s"Iteration $i , save results to $filename")
    (filename, kmer_count)

  }

  def make_reads_rdd_bk(file: String, format: String, n_partition: Int, sc: SparkContext): RDD[String] = {
    if (format.equals("parquet")) throw new NotImplementedError
    else {
      val textRDD = if (n_partition > 0)
        sc.textFile(file, minPartitions = n_partition)
      else
        sc.textFile(file)

      if (format.equals("seq")) {

        textRDD.map {
          line => line.split("\t|\n")
        }.map { x => x(2) }
      }
      else if (format.equals("base64")) {

        textRDD.map {
          line =>
            line.split(",").drop(1).map {
              t =>
                val lst = t.split(" ")
                val len = lst(0).toInt
                DNASeq.from_base64(lst(1)).to_bases(len)
            }.mkString("N")
        }

      }
      else
        throw new IllegalArgumentException
    }

  }

  def contamination(config: Config): Double = {
    if (config.sample_fraction > 0) {
      val oldval = config._contamination
      oldval / config.sample_fraction
    } else {
      config._contamination
    }
  }

  def run(config: Config, sc: SparkContext): Unit = {
    if (config.sample_fraction > 0) {
      logger.info(s"Since random sample is applied, adjust contamination from ${config._contamination} to ${contamination(config)}")
    }

    // read seq file, Hash readID to integer
    // read all sample files
    // determine the partition size
    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")

    val seqFiles = Utils.get_files(config.input.trim(), config.pattern.trim())
    logger.debug(seqFiles)

    val smallReadsRDD = KmerMapReads.make_reads_rdd(seqFiles, config.format, config.n_iteration, config.sample_fraction, sc).map(_._2)
    //val smallReadsRDD = make_reads_rdd_bk(seqFiles, config.format, config.n_partition, sc)
    smallReadsRDD.cache()
    val (filenames, tmp) = 0.until(config.n_iteration).map {
      i =>
        generate_for_iteration(i, smallReadsRDD, config)
    }.unzip
    smallReadsRDD.unpersist()

    val kmer_count = tmp.foldLeft(0l)(_ + _)
    val topN = (kmer_count * contamination(config)).toInt
    logger.info(s"total number of kmers: $kmer_count, take top $topN")


    val result = filenames.map(Utils.unserialize_object).flatMap(_.asInstanceOf[Array[(DNASeq, Int)]]).sortBy(x => (-x._2, x._1.hashCode)).take(topN).toArray


    Utils.write_textfile(config.output, result.map(x => x._1.to_base64 + " " + x._2.toString))


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
        if (config.input.startsWith("hdfs:") && config.n_partition > 0) {
          logger.error("do not set partition when use hdfs input. Change block size of hdfs instead")
          sys.exit(-1)
        }

        logger.info(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark Kmer Counting")
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
