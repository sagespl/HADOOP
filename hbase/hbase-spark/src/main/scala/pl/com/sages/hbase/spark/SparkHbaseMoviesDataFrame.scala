package pl.com.sages.hbase.spark

import java.util.Date

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import pl.com.sages.hbase.api.dao.MovieDao
import pl.com.sages.hbase.api.util.HbaseConfigurationFactory

object SparkHbaseMoviesDataFrame {

  val inputTable = "radek:movies"
  val outputTable = "radek:movies_action"

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName + new Date())
    val spark: SparkSession = SparkSession.builder.master("local").config(conf).getOrCreate
    val sc = spark.sparkContext

    // run
    val hbaseConf = HbaseConfigurationFactory.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, inputTable)

    val inputTableRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val moviesRdd = inputTableRdd.map(result => MovieDao.createMovie(result._2))

    // row rdd
    val value: RDD[Row] = moviesRdd.map(movie => Row(movie.getMovieId.toString, movie.getTitle, movie.getGenres))

    // dataset / dataframe
    val schema = new StructType()
      .add(StructField("id", StringType, true))
      .add(StructField("title", StringType, true))
      .add(StructField("genres", StringType, true))

    val df = spark.createDataFrame(value, schema)
    df.show()

    // end
    sc.stop()
  }

}
