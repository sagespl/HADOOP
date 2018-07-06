package pl.com.sages.spark.core

object SparkPairRddBasic extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val sc = createSparkContext

    // run

    // creating RDD
    val pairs = Map((1, "Ala ma kota"), (2, "Witaj świecie"), (3, "Dwadzieścia tysięcy mil podmorskiej żeglugi"))
    val pairsRdd = sc.parallelize(pairs.toSeq)
    pairsRdd.take(10).foreach(println)

    // map
    val lines = List("Ala ma kota", "Witaj świecie", "Dwadzieścia tysięcy mil podmorskiej żeglugi")
    val linesRdd = sc.parallelize(lines)
    val lineLengthRdd = linesRdd.map(line => (line, line.split(" ").size))
    lineLengthRdd.take(10).foreach(println)

    // mapValues
    val persons = List(("Ala", 3), ("Tomek", 4), ("Kasia", 5), ("Ala", 5), ("Tomek", 3), ("Kasia", 4))
    val personsRdd = sc.parallelize(persons)
    val countRdd = personsRdd.mapValues(v => (v, 1))
    countRdd.take(10).foreach(println)

    // groupByKey
    val groupByKeyRdd = personsRdd.groupByKey()
    groupByKeyRdd.take(10).foreach(println)
    groupByKeyRdd.take(10).foreach(x => println(x._2.toList))

    // reduceByKey
    val mapRdd = personsRdd.mapValues(v => (v, 1))
    val reduceRdd = mapRdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val avgRdd = reduceRdd.mapValues(x => x._1.toDouble / x._2.toDouble)
    avgRdd.take(10).foreach(println)

    // sortByKey
    val sort = personsRdd.sortByKey()
    sort.take(10).foreach(println)

    // countByKey
    val count = personsRdd.countByKey()
    print(count)

    // join
    val x = sc.parallelize(List(("a", 1), ("b", 4)))
    val y = sc.parallelize(List(("a", 2), ("a", 3)))
    x.join(y).take(10).foreach(println)
    x.leftOuterJoin(y).take(10).foreach(println)
    x.rightOuterJoin(y).take(10).foreach(println)
    x.cogroup(y).take(10).foreach(println)

    // MapReduce v1
    val booksRdd = sc.textFile(bookPath)
    val wordsRdd = booksRdd.flatMap(line => line.split(" "))
    val wordCount1 = wordsRdd.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    wordCount1.take(20).foreach(println)

    // MapReduce v2
    val wordCount2 = wordsRdd.countByValue()
    wordCount2.take(20).foreach(println)

    // end
    sc.stop()
  }

}
