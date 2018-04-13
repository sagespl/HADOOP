package pl.com.sages.hbase.spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import pl.com.sages.hbase.api.dao.MovieDao
import pl.com.sages.hbase.api.util.HbaseConfigurationFactory

object SparkHBase {

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName + "5")
    val sc = new SparkContext(conf)

    val tableName = "sages:movies"

    // run
    val hbaseConf = HbaseConfigurationFactory.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    // test
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf(tableName))

    val scan: Scan = new Scan().setMaxVersions(10)
    val scanner: ResultScanner = table.getScanner(scan)
    import scala.collection.JavaConversions._
    for (result <- scanner) {
      println(result.toString)
    }

    val hbaseTableRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val moviesRdd = hbaseTableRdd.map(result => MovieDao.createMovie(result._2))
    val genresRdd = moviesRdd.flatMap(movie => movie.getGenres.split("\\|"))
    genresRdd.distinct().saveAsTextFile("/user/sages/hbase-spark/result")

    // end
    sc.stop()
  }

}
