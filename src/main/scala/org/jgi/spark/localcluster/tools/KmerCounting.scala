/**
  * Created by Lizhen Shi on 5/13/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.{DNASeq, JedisManager, Kmer, Utils}
import sext._


object KmerCounting extends LazyLogging {

  case class Config(input: String = "", output: String = "", n_iteration: Int = 1, pattern: String = "",
                    _contamination: Double = 0.00005, k: Int = 31, format: String = "seq", sleep: Int = 0,
                    scratch_dir: String = "/tmp", n_partition: Int = 0, sample_fraction: Double = -1.0,
                    redis_ip: String = "", redis_port: Int = 0, use_redis: Boolean = false)

  var jedisManager = None: Option[JedisManager]

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("KmerCounting") {
      head("kmer counting", Utils.VERSION)

      opt[String]('i', "input").required().valueName("<dir>").action((x, c) =>
        c.copy(input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]('p', "pattern").valueName("<pattern>").action((x, c) =>
        c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")


      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")

      opt[String]("redis").valueName("<ip:port>").validate { x =>
        if (Utils.parseIpPort(x)._2 < 1)
          failure("format is not correct")
        else
          success

      }.action { (x, c) =>
        val r = Utils.parseIpPort(x)
        c.copy(redis_ip = r._1, redis_port = r._2, use_redis = true)
      }.text("ip:port for a node in redis cluster. Only IP supported.")


      opt[String]("format").valueName("<format>").action((x, c) =>
        c.copy(format = x)).
        validate(x =>
          if (List("seq", "parquet", "base64").contains(x)) success
          else failure("only valid for seq, parquet or base64")
        ).text("input format (seq, parquet or base64)")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]('k', "kmer_length").action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Int]("n_iteration").action((x, c) =>
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
    val conf = new Configuration()

    val output = new Path(filepath)
    val hdfs = FileSystem.get(conf)

    // delete existing directory
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }
  }

  def getJedisManager(config: Config): JedisManager = {
    jedisManager match {
      case Some(x) =>
        x
      case None =>
        val x = new JedisManager(config.redis_ip, config.redis_port)
        jedisManager = Some(x)
        x
    }
  }

  def flushAll(config: Config): Unit = {
    val mgr = getJedisManager(config)
    mgr.flushAll()
  }

  private def process_iteration_redis(i: Int, readsRDD: RDD[String], config: Config, sc: SparkContext): RDD[(DNASeq, Int)] = {
    readsRDD.foreachPartition {
      iterator =>
        val cluster = getJedisManager(config).getJedisCluster
        iterator.foreach {
          line =>
            Kmer.generate_kmer(seq = line, k = config.k)
              .filter(o => Utils.pos_mod(o.hashCode, config.n_iteration) == i).map(_.to_base64).foreach {
              str =>
                cluster.incr(str)
            }

        }
      //cluster.close()
    }

    import com.redislabs.provider.redis._
    sc.fromRedisKV("*").map(x => (DNASeq.from_base64(x._1), x._2.toInt))
  }

  private def process_iteration(i: Int, readsRDD: RDD[String], config: Config, sc: SparkContext) = {
    val smallKmersRDD = if (config.use_redis)
      process_iteration_redis(i, readsRDD, config, sc)
    else
      process_iteration_spark(i, readsRDD, config)
    val kmer_count = smallKmersRDD.count

    val topN = (kmer_count * contamination(config) * math.min(1.5, config.n_iteration)).toInt
    logger.info(s"Iteration $i , number of kmers: $kmer_count, take top $topN")

    val topKmers = smallKmersRDD.takeOrdered(topN)(Ordering[Int].reverse.on { x => x._2 })

    val rdd = sc.parallelize(topKmers)
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    (rdd, kmer_count)

  }

  private def process_iteration_spark(i: Int, readsRDD: RDD[String], config: Config): RDD[(DNASeq, Int)] = {
    readsRDD.map(x => Kmer.generate_kmer(seq = x, k = config.k)).flatMap(x => x)
      .filter(o => Utils.pos_mod(o.hashCode, config.n_iteration) == i)
      .map((_, 1)).reduceByKey(_ + _)
  }


  def contamination(config: Config): Double = {
    config._contamination
  }

  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")

    val seqFiles = Utils.get_files(config.input.trim(), config.pattern.trim())
    logger.debug(seqFiles)

    val smallReadsRDD = KmerMapReads.make_reads_rdd(seqFiles, config.format, config.n_partition, config.sample_fraction, sc).map(_._2)

    smallReadsRDD.cache()
    if (config.use_redis) flushAll(config)
    val values = 0.until(config.n_iteration).map {
      i =>
        val t = process_iteration(i, smallReadsRDD, config, sc)
        if (config.use_redis) flushAll(config)
        t
    }
    smallReadsRDD.unpersist()

    if (true) {
      //hdfs
      val rdds=values.map(_._1)
      KmerCounting.delete_hdfs_file(config.output)
      val rdd = sc.union(rdds).reduceByKey(_ + _)
      val kmer_count = values.map(_._2).sum
      val topN = (kmer_count * contamination(config)).toInt
      val filteredKmer = rdd.takeOrdered(topN)(Ordering[Int].reverse.on { x => x._2 })
      val filteredKmerRDD = sc.parallelize(filteredKmer).map(x => x._1.to_base64 + " " + x._2.toString)
      filteredKmerRDD.saveAsTextFile(config.output)
      logger.info(s"total #records=${filteredKmerRDD.count}/${kmer_count}/${topN} save results to hdfs ${config.output}")

      //cleanup
      rdds.foreach(_.unpersist())
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
        val conf = new SparkConf().setAppName("Spark Kmer Counting")
        conf.registerKryoClasses(Array(classOf[DNASeq]))
        if (config.use_redis) {
          conf.set("redis.host", config.redis_ip)
            .set("redis.port", config.redis_port.toString)
        }
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
