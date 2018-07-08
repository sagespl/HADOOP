package pl.com.sages.spark.core

trait ClusterApp {

  /**
    * User name
    */
  val user: String = System.getProperty("user.name")

  /**
    * Where to run master process
    */
  val master: String = "yarn"

  val useAws = false

  /**
    * Directory path with sample data
    */
  val dataPath: String = "/user/sages/dane"

  /**
    * Output path (results)
    */
  val resultPath: String = "/user/" + user + "/wyniki/spark"

}
