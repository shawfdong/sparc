/**
  * Created by Lizhen Shi on 5/13/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster._
import org.jgi.spark.localcluster.kvstore.{KVStoreManager, KVStoreManagerSingleton}
import org.jgi.spark.localcluster.myredis.{JedisManager, JedisManagerSingleton}
import sext._


object KmerCounting extends App with LazyLogging {

  case class Config(input: String = "", output: String = "", n_iteration: Int = 1, pattern: String = "",
                    _contamination: Double = 0.00005, k: Int = 31, format: String = "seq", sleep: Int = 0,
                    scratch_dir: String = "/tmp", n_partition: Int = 0,
                    use_redis: Boolean = false, redis_ip_ports: Array[(String, Int)] = null, n_redis_slot: Int = 2, //redis config, to be removed
                    user_kvstore: Boolean = false, kvstore_ip_ports: Array[(String, Int)] = null,
                    use_bloom_filter: Boolean = false)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("KmerCounting") {
      head("kmer counting", Utils.VERSION)

      opt[String]('i', "input").required().valueName("<dir>").action((x, c) =>
        c.copy(input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]('p', "pattern").valueName("<pattern>").action((x, c) =>
        c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")


      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")

      opt[String]("kvstore").valueName("<ip:port,ip:port,...>").validate { x =>
        val t = x.split(",").map {
          u =>
            if (Utils.parseIpPort(u)._2 < 1) 1 else 0
        }.sum
        if (t > 0)
          failure("format is not correct")
        else
          success
      }.action { (x, c) =>
        val r = x.split(",").map(Utils.parseIpPort)
        c.copy(kvstore_ip_ports = r, user_kvstore = true)
      }.text("ip:port for kvstore server. Only IP supported.")

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

      opt[Unit]("use_bloom_filter").action((x, c) =>
        c.copy(use_bloom_filter = true))
        .text("use bloomer filter")


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
    JedisManagerSingleton.instance(config.redis_ip_ports, config.n_redis_slot)
  }

  def getKVStoreManager(config: Config): KVStoreManager = {
    KVStoreManagerSingleton.instance(config.kvstore_ip_ports)
  }


  def redisFlushAll(config: Config): Unit = {
    val mgr = getJedisManager(config)
    mgr.flushAll()
  }

  private def process_iteration_kvstore(i: Int, readsRDD: RDD[String], config: Config, sc: SparkContext): RDD[(DNASeq, Int)] = {
    val THRESH_HOLD = 1024 * 32
    readsRDD.foreachPartition {
      iterator =>
        val buf = scala.collection.mutable.ArrayBuffer.empty[DNASeq]
        val cluster = getKVStoreManager(config)

        def incr_fun(u: collection.Iterable[DNASeq]) = cluster.incr_batch(u, config.use_bloom_filter)

        iterator.foreach {
          line =>
            Kmer.generate_kmer(seq = line, k = config.k)
              .filter(o => Utils.pos_mod(o.hashCode, config.n_iteration) == i).foreach {
              s =>
                buf.append(s)
                if (buf.length > THRESH_HOLD) {
                  incr_fun(buf)
                  buf.clear()
                }
            }
        }
        //remained
        if (buf.nonEmpty) {
          incr_fun(buf)
          buf.clear()
        }
    }
    val cluster = getKVStoreManager(config)
    import org.jgi.spark.localcluster.rdd._
    sc.kmerCountFromKVStore(cluster.kvstoreSlots, useBloomFilter = config.use_bloom_filter, minimumCount = 1)


  }

  private def process_iteration_redis(i: Int, readsRDD: RDD[String], config: Config, sc: SparkContext): RDD[(DNASeq, Int)] = {
    val THRESH_HOLD = 1024 * 32
    readsRDD.foreachPartition {
      iterator =>
        val buf = scala.collection.mutable.ArrayBuffer.empty[DNASeq]
        val cluster = getJedisManager(config)

        def incr_fun(u: collection.Iterable[DNASeq]) = if (config.use_bloom_filter) cluster.bf_incr_batch(u) else cluster.incr_batch(u)

        iterator.foreach {
          line =>
            Kmer.generate_kmer(seq = line, k = config.k)
              .filter(o => Utils.pos_mod(o.hashCode, config.n_iteration) == i).foreach {
              s =>
                buf.append(s)
                if (buf.length > THRESH_HOLD) {
                  incr_fun(buf)
                  buf.clear()
                }
            }
        }
        //remained
        if (buf.nonEmpty) {
          incr_fun(buf)
          buf.clear()
        }
    }
    val cluster = getJedisManager(config)
    import org.jgi.spark.localcluster.rdd._
    if (config.use_bloom_filter)
      sc.kmerCountFromRedisWithBloomFilter(cluster.redisSlots)
    else
      sc.kmerCountFromRedis(cluster.redisSlots)

  }

  private def process_iteration(i: Int, readsRDD: RDD[String], config: Config, sc: SparkContext) = {
    val smallKmersRDD = if (config.use_redis)
      process_iteration_redis(i, readsRDD, config, sc)
    else if (config.user_kvstore)
      process_iteration_kvstore(i, readsRDD, config, sc)
    else
      process_iteration_spark(i, readsRDD, config)
    val rdd = smallKmersRDD.filter(_._2 > 1)
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val kmer_count = rdd.count
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

    val smallReadsRDD = KmerMapReads.make_reads_rdd(seqFiles, config.format, config.n_partition, -1, sc).map(_._2)

    smallReadsRDD.cache()
    if (config.use_redis) redisFlushAll(config)
    val values = 0.until(config.n_iteration).map {
      i =>
        val t = process_iteration(i, smallReadsRDD, config, sc)
        if (config.use_redis) redisFlushAll(config)
        t
    }

    if (true) {
      //hdfs
      val rdds = values.map(_._1) //WARNING make sure the rdds in the list are exclusive for kmers
      KmerCounting.delete_hdfs_file(config.output)
      if (false) { //take tops
        val rdd = sc.union(rdds)
        val kmer_count = values.map(_._2).sum
        val topN = (kmer_count * contamination(config)).toInt
        val filteredKmer = rdd.takeOrdered(topN)(Ordering[Int].reverse.on { x => x._2 })
        val filteredKmerRDD = sc.parallelize(filteredKmer).map(x => x._1.to_base64 + " " + x._2.toString)
        filteredKmerRDD.saveAsTextFile(config.output)
        logger.info(s"total #records=${filteredKmerRDD.count}/${kmer_count}/${topN} save results to hdfs ${config.output}")

      } else { //exclude top kmers and 1 kmers
        val rdd = sc.union(rdds)
        val kmer_count = values.map(_._2).sum //all distinct kmer count (include len 1 kmern and top kmers
        val topN = (kmer_count * contamination(config)).toInt
        val takeN = rdd.count.toInt - topN
        val filteredKmer = rdd.filter(x => x._2 > 1).takeOrdered(takeN)(Ordering[Int].on { x => x._2 })

        val filteredKmerRDD = sc.parallelize(filteredKmer).map(x => x._1.to_base64 + " " + x._2.toString)
        filteredKmerRDD.saveAsTextFile(config.output)
        logger.info(s"total #kmer/topN=${kmer_count}/${topN} save results to hdfs ${config.output}")
      }

      //cleanup
      smallReadsRDD.unpersist()
      rdds.foreach(_.unpersist())
      val totalTime1 = System.currentTimeMillis
      logger.info("kmer counting time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
    }
  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

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
