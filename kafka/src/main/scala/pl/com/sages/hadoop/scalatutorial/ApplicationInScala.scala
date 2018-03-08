package pl.com.sages.hadoop.scalatutorial

import scala.io.Source

/**
  * Created by radek on 04.02.17.
  */
object ApplicationInScala extends App {

  println("To jest wnętrze naszej aplikacji")

  val source = Source.fromFile("/home/radek/Sages/repository/HADOOP/mapreduce/src/test/resources/20-000-mil-podmorskiej-zeglugi.txt", "UTF-8")

  val lineIterator = source.getLines

  val lines = source.getLines.toArray

  for (line <- lines) println(line)

}
