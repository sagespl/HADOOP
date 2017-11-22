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

    // sample
    val sample = nums.sample(false, 0.5)
    println(sample.collect().mkString(", "))

    // union, intersection, subtract, cartesian
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(List(4, 5, 6, 7))

    val union = rdd1.union(rdd2)
    println(union.collect().mkString(", "))

    val intersection = rdd1.intersection(rdd2)
    println(intersection.collect().mkString(", "))

    val subtract = rdd1.subtract(rdd2)
    println(subtract.collect().mkString(", "))

    val cartesian = rdd1.cartesian(rdd2)
    println(cartesian.collect().mkString(", "))

    // end
    sc.stop()
  }

}
