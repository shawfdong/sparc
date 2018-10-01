package net.sparc.graph

import java.util.UUID.randomUUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, Row}

class Checkpoint(val prefix: String, val checkpoint_dir: String) extends LazyLogging {

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


  def remove_all() = {
    checkpoints.foreach(delete_hdfs_file)
    checkpoints.clear();
  }

  def checkpoint(df: Dataset[Row], rm_prev_ckpt: Boolean = true): (String, Dataset[Row]) = {
    val ckpt_path = checkpoint_dir + "/" + prefix + "_" + randomUUID() + ".ckpt"
    logger.info(s"Checkpoint to ${ckpt_path}")
    df.write.parquet(ckpt_path)

    if (rm_prev_ckpt) {
      remove_all()
      checkpoints.add(ckpt_path);
    }
    (ckpt_path, df.sqlContext.read.parquet(ckpt_path))
  }

  private val checkpoints: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty[String];

}
