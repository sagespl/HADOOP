package pl.com.sages.hbase.spark

import java.util.Date

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import pl.com.sages.hbase.api.dao.MovieDao
import pl.com.sages.hbase.api.util.HbaseConfigurationFactory

object SparkHbaseMoviesFilter {

  val tableName = "sages:movies"

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName + new Date())
    val sc = new SparkContext(conf)

    // run
    val hbaseConf = HbaseConfigurationFactory.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseTableRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val moviesRdd = hbaseTableRdd.map(result => MovieDao.createMovie(result._2))
    val genresRdd = moviesRdd.flatMap(movie => movie.getGenres.split("\\|")).distinct

    // delete result directory and save result on HDFS
    val resultPath = "/user/sages/hbase-spark/result"
    import org.apache.hadoop.fs.{FileSystem, Path}
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(resultPath), true)
    genresRdd.coalesce(1).saveAsTextFile(resultPath)

    // end
    sc.stop()
  }

}
