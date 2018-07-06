package pl.com.sages.spark.sql

import pl.com.sages.spark.core.BaseSparkApp

object SparkDatasetBasic extends BaseSparkSqlApp with BaseSparkApp {

  def main(args: Array[String]): Unit = {

    // prepare
    val spark = createSparkSession
    import spark.implicits._

    // run

    // creating RDD
    val foodDs = List("pizza", "hamburger", "lasagne").toDS()
    foodDs.show

    // map
    val nums = List(1, 2, 2, 3, 4, 4, 5).toDS()
    val squared = nums.map(x => x * x)
    println(squared.collect().mkString("\n"))

    // flatMap
    val lines = List("Ala ma kota", "Witaj świecie", "Dwadzieścia tysięcy mil podmorskiej żeglugi").toDS()
    val words = lines.flatMap(_.split(" "))
    println(words.collect().mkString("\n"))

    // filter
    val filteredLines = lines.filter(line => line.contains("kot"))
    println(filteredLines.collect().mkString("\n"))

    // distinct
    val distinct = nums.distinct()
    println(distinct.collect().mkString(", "))

    // sample
    val sample = nums.sample(false, 0.5)
    println(sample.collect().mkString(", "))

    // union, intersection, subtract, cartesian
    val ds1 = List(1, 2, 3, 4, 5).toDS()
    val ds2 = List(4, 5, 6, 7).toDS()

    val union = ds1.union(ds2)
    println(union.collect().mkString(", "))

    val intersection = ds1.intersect(ds2)
    println(intersection.collect().mkString(", "))

    val subtract = ds1.except(ds2)
    println(subtract.collect().mkString(", "))

    val cross = ds1.crossJoin(ds2)
    println(cross.collect().mkString(", "))

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    val cartesian = ds1.join(ds2)
    println(cartesian.collect().mkString(", "))

    // reduce
    val reduceSum = nums.reduce((x, y) => x + y)
    println(reduceSum)

    // count & countByValue
    val count = nums.count()
    print(count)

    // foreach
    nums.foreach(x => println(x))

    // actions
    nums.first()
    nums.take(3)

    // end
    spark.stop()
  }

}
