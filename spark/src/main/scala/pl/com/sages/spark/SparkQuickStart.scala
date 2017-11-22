package pl.com.sages.spark

import org.apache.spark.sql.SparkSession

object SparkQuickStart {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkQuickStart")
      .enableHiveSupport()
      .getOrCreate()

    // Implicit methods available in Scala for converting common Scala objects into DataFrames/Datasets
    import spark.implicits._

    spark.version

    // reading from HDFS
    val textFile = spark.read.textFile("/user/sages/dane/lektury-100")

    textFile.count()
    textFile.first()

    // filtering lines
    val filteredDataset = textFile.filter(line => line.contains("wolnelektury"))
    filteredDataset.count()
    filteredDataset.cache()
    filteredDataset.count()

    // longest line in dataset (simple MapReduce)
    textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)

    // word count (traditional MapReduce)
    val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count()
    wordCounts.show(10)
    wordCounts.sort($"count(1)".desc).show(10)
  }

}