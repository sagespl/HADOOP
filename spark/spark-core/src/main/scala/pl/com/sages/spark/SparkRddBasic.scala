package pl.com.sages.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkRddBasic extends GlobalParameters {

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // run

    // creating RDD
    val foodRdd = sc.parallelize(List("pizza", "hamburger", "lasagne"))

    // map
    val nums = sc.parallelize(List(1, 2, 2, 3, 4, 4, 5))
    val squared = nums.map(x => x * x)
    println(squared.collect().mkString("\n"))

    // flatMap
    val phraseRdd = sc.parallelize(List("Ala ma kota", "Witaj świecie", "Dwadzieścia tysięcy mil podmorskiej żeglugi"))
    val words = phraseRdd.flatMap(line => line.split(" "))
    println(words.collect().mkString("\n"))

    // filter
    val filteredLines = phraseRdd.filter(line => line.contains("kot"))
    filteredLines.count()
    filteredLines.first()

    // distinct
    val distinct = nums.distinct()
    println(distinct.collect().mkString(", "))

    // end
    sc.stop()
  }

}
