package pl.com.sages.spark.sql

import pl.com.sages.spark.GlobalParameters

object SparkDatasetSampleApp extends BaseSparkSqlApp with GlobalParameters {

  def main(args: Array[String]) {

    val spark = createSparkSession

    val fileDataset = spark.read.textFile(bookPath).cache()
    val numAs = fileDataset.filter(line => line.contains("a")).count()
    val numBs = fileDataset.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }

}
