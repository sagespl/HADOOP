package pl.com.sages.spark.sql

import pl.com.sages.spark.core.BaseSparkApp

object SparkDatasetSampleApp extends BaseSparkSqlApp with BaseSparkApp {

  def main(args: Array[String]) {

    val spark = createSparkSession

    val fileDataset = spark.read.textFile(bookPath).cache()
    val numAs = fileDataset.filter(line => line.contains("a")).count()
    val numBs = fileDataset.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }

}
