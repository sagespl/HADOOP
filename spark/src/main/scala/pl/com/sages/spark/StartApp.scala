package pl.com.sages.spark

import org.apache.spark.sql.SparkSession

object StartApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      .enableHiveSupport()
      .getOrCreate()

    val textFile = spark.read.textFile("/user/sages/dane/lektury-100")

    textFile.count()
    textFile.first()

    val linesWithSpark = textFile.filter(line => line.contains("wolnelektury"))
    linesWithSpark.count()

    textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
  }

}
