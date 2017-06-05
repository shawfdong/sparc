/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.myredis.{JedisManager, JedisManagerSingleton}
import org.jgi.spark.localcluster.{DNASeq, SingleEdge, Utils}
import org.jgi.spark.localcluster.myredis._
import sext._

object GraphGen  extends App with  LazyLogging {

  case class Config(kmer_reads: String = "", output: String = "",
                    n_iteration: Int = 1, k: Int = -1, min_shared_kmers: Int = 2, sleep: Int = 0,
                    scratch_dir: String = "/tmp", n_partition: Int = 0,
                    use_redis: Boolean = false, redis_ip_ports: Array[(String, Int)] = null, n_redis_slot: Int = 2,
                    use_bloom_filter: Boolean = false)


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

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 2) success
          else failure("min_shared_kmers should be greater than 2"))
        .text("minimum number of kmers that two reads share")


      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")

      opt[String]("redis").valueName("<ip:port,ip:port,...>").validate { x =>
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
        c.copy(redis_ip_ports = r, use_redis = true)
      }.text("ip:port for redis servers. Only IP supported.")

      opt[Unit]("use_bloom_filter").action((x, c) =>
        c.copy(use_bloom_filter = true))
        .text("use bloomer filter. only applied to redis")


      opt[String]("scratch_dir").valueName("<dir>").action((x, c) =>
        c.copy(scratch_dir = x)).text("where the intermediate results are")


      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  private def process_iteration_spark(i: Int, kmer_reads: RDD[Array[Long]], config: Config, sc: SparkContext): RDD[(Int, Int, Int)] = {
    val edges = kmer_reads.map(_.combinations(2).map(x => x.sorted).filter {
      case a =>
        Utils.pos_mod((a(0) + a(1)).toInt, config.n_iteration) == i
    }.map(x => (x(0), x(1)))).flatMap(x => x)

    val groupedEdges = edges.countByValue().filter(x => x._2 >= config.min_shared_kmers)
      .map(x => (x._1._1.toInt, x._1._2.toInt, x._2.toInt)).toList

    val rdd = sc.parallelize(groupedEdges)
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd

  }

  def getJedisManager(config: Config): JedisManager = {
    JedisManagerSingleton.instance(config.redis_ip_ports, config.n_redis_slot)
  }

  private def process_iteration_redis(i: Int, kmer_reads: RDD[Array[Long]], config: Config, sc: SparkContext): RDD[(Int, Int, Int)] = {
    val THRESH_HOLD = 1024 * 32

    kmer_reads.foreachPartition {
      iterator =>
        val buf = scala.collection.mutable.ArrayBuffer.empty[SingleEdge]
        val cluster = getJedisManager(config)

        def incr_fun(u: collection.Iterable[SingleEdge]) =
          if (config.use_bloom_filter) cluster.bf_incr_edge_batch(u) else cluster.incr_edge_batch(u)

        iterator.foreach {
          lst =>
            lst.combinations(2).map(x => x.sorted).filter {
              case a =>
                Utils.pos_mod((a(0) + a(1)).toInt, config.n_iteration) == i
            }.map(x => new SingleEdge(x(0), x(1))).foreach(x => buf.append(x))
            if (buf.length > THRESH_HOLD) {
              incr_fun(buf)
              buf.clear()
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
    val results = (if (config.use_bloom_filter)
      sc.edgeCountFromRedisWithBloomFilter(cluster.redisSlots)
    else
      sc.edgeCountFromRedis(cluster.redisSlots).filter(x => x._2 >= config.min_shared_kmers)
      ).filter(x => x._2 >= config.min_shared_kmers).map(x => (x._1.src, x._1.dest, x._2))
    results.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logger.info(s"iteration $i, #records ${results.count}")
    results
  }

  def flushAll(config: Config): Unit = {
    val mgr = getJedisManager(config)
    mgr.flushAll()
  }

  private def process_iteration(i: Int, kmer_reads: RDD[Array[Long]], config: Config, sc: SparkContext): RDD[(Int, Int, Int)] = {
    if (config.use_redis)
      process_iteration_redis(i, kmer_reads, config, sc)
    else
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
          line.split(" ").apply(1).split(",").map(_.toLong)
        }

    kmer_reads.cache()
    logger.info("loaded %d kmer-reads-mapping".format(kmer_reads.count))
    kmer_reads.take(5).map(_.mkString(",")).foreach(x => logger.info(x))

    if (config.use_redis) flushAll(config)
    val values = 0.until(config.n_iteration).map {
      i =>
        val r = process_iteration(i, kmer_reads, config, sc)
        if (config.use_redis) flushAll(config)
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
	}catch {
	case  e:Exception => e.printStackTrace()
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
