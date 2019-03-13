package pl.com.sages.spark.core

trait LocalApp {

  /**
    * User name
    */
  val user: String = System.getProperty("user.name")

  /**
    * Where to run master process
    */
  val master: String = "local[*]"

  /**
    * Directory path with sample data
    */
  val dataPath: String = System.getenv("HADOOP_DATA")

  /**
    * Output path (results)
    */
  val resultPath: String = "/tmp/" + user + "/wyniki/spark"

}
