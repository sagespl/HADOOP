package pl.com.sages.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import pl.com.sages.spark.core.BaseSparkApp

trait BaseSparkSqlApp extends BaseSparkApp {

  def createSparkSession: SparkSession = {

    val conf = new SparkConf().setAppName(user + " " + this.getClass.getSimpleName)

    SparkSession.builder().
      config(conf).
      master(master).
      getOrCreate()

  }

}
