package pl.com.sages.spark.core

trait ClusterApp {

  /**
    * User name
    */
  val user: String = "sages"

  /**
    * Where to run master process
    */
  val master: String = "yarn"

  /**
    * Directory path with sample data
    */
  val dataPath: String = "/dane"

  /**
    * Output path (results)
    */
  val resultPath: String = "/user/" + user + "/wyniki/spark"

}
