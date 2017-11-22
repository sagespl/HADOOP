package pl.com.sages.spark

import org.apache.spark.sql.SparkSession

object SparkQuickStartApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val logData = spark.read.textFile("/user/sages/dane/lektury-100").cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }

}
