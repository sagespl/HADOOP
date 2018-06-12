package pl.com.sages.spark

import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait BaseSparkSqlApp extends GlobalParameters {

  def createSparkSession: SparkSession = {

    // AWS
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    val fsImpl = classOf[S3AFileSystem].getCanonicalName

    val conf = new SparkConf().setAppName("Sages " + this.getClass.getSimpleName)

    if (useAws) {
      conf.set("spark.hadoop.fs.s3a.access.key", accessKeyId).
        set("spark.hadoop.fs.s3a.secret.key", secretAccessKey).
        set("spark.hadoop.fs.s3a.impl", fsImpl)
    }

    val spark = SparkSession.builder().
      config(conf).
      master("local").
      getOrCreate()

    val sc = spark.sparkContext

    if (useAws) {
      // Hadoop conf
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
    }

    spark
  }

}
