package pl.com.sages.spark.sql

import pl.com.sages.spark.core.BaseSparkApp

object SparkDatasetQuickStart extends BaseSparkSqlApp with BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession

    // Implicit methods available in Scala for converting common Scala objects into DataFrames/Datasets
    import spark.implicits._

    // reading from HDFS
    val textFile = spark.read.textFile(bookPath)

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

    // end
    spark.stop()
  }

}