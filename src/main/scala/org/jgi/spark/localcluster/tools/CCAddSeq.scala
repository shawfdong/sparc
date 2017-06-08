/**
  * Created by Lizhen Shi on 6/8/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.tools.KmerMapReads.make_reads_rdd
import org.jgi.spark.localcluster.{DNASeq, Utils}
import sext._


object CCAddSeq extends App with LazyLogging {

  case class Config(cc_file: String = "", output: String = "",
                    sleep: Int = 0, reads_input: String = "", pattern: String = "", format: String = "seq",
                    scratch_dir: String = "/tmp", n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("CCAddSeq") {
      head("GraphCC", Utils.VERSION)

      opt[String]('i', "cc_file").required().valueName("<file>").action((x, c) =>
        c.copy(cc_file = x)).text("files of graph edges. e.g. output from GraphCC")

      opt[String]("reads").required().valueName("<dir|file>").action((x, c) =>
        c.copy(reads_input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]('p', "pattern").valueName("<pattern>").action((x, c) =>
        c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")

      opt[String]("format").valueName("<format>").action((x, c) =>
        c.copy(format = x)).
        validate(x =>
          if (List("seq", "parquet", "base64").contains(x)) success
          else failure("only valid for seq, parquet or base64")
        ).text("input format (seq, parquet or base64)")


      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[String]("scratch_dir").valueName("<dir>").action((x, c) =>
        c.copy(scratch_dir = x)).text("where the intermediate results are")


      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }


  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")

    val ccRDD = (if (config.n_partition > 0)
      sc.textFile(config.cc_file, minPartitions = config.n_partition)
    else
      sc.textFile(config.cc_file))
      .map(_.split(",")).map(x => x.map(_.toInt)).zipWithIndex().map {
      case (nodes, idx) =>
        nodes.map((_, idx.toInt))
    }.flatMap(x => x)


    val seqFiles = Utils.get_files(config.reads_input.trim(), config.pattern.trim())
    logger.debug(seqFiles)
    val readsRDD = make_reads_rdd(seqFiles, config.format, config.n_partition, sc).map(x => (x._1.toInt, x._2))
    val resultRDD = ccRDD.join(readsRDD).map(_._2).map(x => x._1.toString + "\t" + x._2).coalesce(1, shuffle = false)
    resultRDD.saveAsTextFile(config.output)


    logger.info(s"save results to ${config.output}")

    val totalTime1 = System.currentTimeMillis
    logger.info("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))


  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark CCAddSeq")
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
