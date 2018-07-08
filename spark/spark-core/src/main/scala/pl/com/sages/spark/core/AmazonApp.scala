package pl.com.sages.spark.core

trait AmazonApp {

  /**
    * User name
    */
  val user: String = System.getProperty("user.name")

  /**
    * Where to run master process
    */
  val master: String = "yarn"

  val useAws = true

  /**
    * Directory path with sample data
    */
  val dataPath: String = "s3a://sages-aws/dane"

  /**
    * Output path (results)
    */
  val resultPath: String = "s3a://sages-aws/wyniki/spark"

}
