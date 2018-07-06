package pl.com.sages.spark.sql.streaming

import java.util.UUID

import pl.com.sages.spark.core.BaseSparkApp
import pl.com.sages.spark.sql.BaseSparkSqlApp

object StructuredKafkaWordCount extends BaseSparkSqlApp with BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = createSparkSession
    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopics)
      .option("group.id", kafkaGroupId)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }

}