package pl.com.sages.spark

import org.apache.spark.sql.SparkSession

object SparkDatasetSampleApp extends GlobalParameters {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName(this.getClass.getSimpleName).getOrCreate()

    val fileDataset = spark.read.textFile(bookPath).cache()
    val numAs = fileDataset.filter(line => line.contains("a")).count()
    val numBs = fileDataset.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }

}
