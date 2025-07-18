package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object EventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Customer Event Kafka Avro Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "customer_events"
    val checkpointLocation = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/kafka-consumer-checkpoint"
    val avroOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/raw_events_avro"

    // Load Avro schema from file
    val schemaPath = "src/main/avro/CustomerEventUnion.avsc"
    val avroSchemaJson = new String(Files.readAllBytes(Paths.get(schemaPath)), StandardCharsets.UTF_8)

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    val decoded = kafkaDF
      .selectExpr("CAST(key AS STRING)", "value")
      .withColumn("event", from_avro(col("value"), avroSchemaJson))

    val query = decoded.select("key", "event.*")
      .writeStream
      .format("avro")
      .option("checkpointLocation", checkpointLocation)
      .option("path", avroOutputPath)
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
