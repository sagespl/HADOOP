package pl.com.sages.spark.core

object SparkRddBasic extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val sc = createSparkContext

    // run

    // creating RDD
    val foodRdd = sc.parallelize(List("pizza", "hamburger", "lasagne"))
    foodRdd.collect().foreach(println)

    // map
    val nums = sc.parallelize(List(1, 2, 2, 3, 4, 4, 5))
    val squared = nums.map(x => x * x)
    println(squared.collect().mkString("\n"))

    // flatMap
    val lines = sc.parallelize(List("Ala ma kota", "Witaj świecie", "Dwadzieścia tysięcy mil podmorskiej żeglugi"))
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

    // reduce
    val reduceSum = nums.reduce((x, y) => x + y)
    println(reduceSum)

    // fold
    val foldSum = nums.fold(0)((x, y) => x + y)
    println(foldSum)

    // aggregate
    val aggregateResult = nums.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    println(aggregateResult)
    val aggreagetAvg = aggregateResult._1.toDouble / aggregateResult._2.toDouble
    println(aggreagetAvg)

    // count & countByValue
    val count = nums.count()
    print(count)
    val countByValue = nums.countByValue()
    print(countByValue)

    // foreach
    nums.foreach(x => println(x))
    def f(x:Int) = println(x)
    nums.foreach(f)

    // actions
    nums.first()
    nums.take(3)
    nums.top(3)
    nums.takeSample(true,3)
    nums.takeOrdered(3)
    nums.takeOrdered(3)(Ordering[Int].reverse)

    // end
    sc.stop()
  }

}
