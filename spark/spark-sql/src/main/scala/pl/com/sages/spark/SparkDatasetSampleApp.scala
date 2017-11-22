package pl.com.sages.spark

import org.apache.spark.sql.SparkSession

object SparkDatasetSampleApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Dataset sample App").getOrCreate()

    val fileDataset = spark.read.textFile("/user/sages/dane/lektury-100").cache()
    val numAs = fileDataset.filter(line => line.contains("a")).count()
    val numBs = fileDataset.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }

}
