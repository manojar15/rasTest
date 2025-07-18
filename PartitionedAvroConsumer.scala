package rastest

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object PartitionedAvroFileEventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro File Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val avroInputPath = "file:///C:/Users/E5655076/RAS_RPT/obrandrastest/avro_output/*/*"
    val outputPath = "file:///C:/Users/E5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val checkpointLocation = "file:///C:/Users/E5655076/RAS_RPT/obrandrastest/customer/checkpoints_file_consumer"

    // Read schema for CustomerEvent
    val wrapperSchemaPath = "src/main/avro/CustomerEvent.avsc"
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get(wrapperSchemaPath)), StandardCharsets.UTF_8)

    // Read Avro files as a streaming source
    val rawDF = spark.readStream
      .format("avro")
      .load(avroInputPath)

    // Deserialize using Avro schema
    val parsedDF = rawDF
      .withColumn("event", from_avro(col("value"), wrapperSchemaJson))
      .withColumn("header", col("event.header"))
      .withColumn("customer_id", col("header.entity_id"))
      .withColumn("event_type", col("header.event_type"))
      .withColumn("tenant_id", col("header.tenant_id"))
      .withColumn("logical_date", col("header.logical_date").cast("date"))  // Cast properly
      .withColumn("partition_id", split(col("header.entity_id"), "_")(0))   // Partition ID logic
      .select(
        col("partition_id"),
        col("tenant_id"),
        col("customer_id"),
        col("event_type"),
        col("logical_date"),
        col("value")
      )

    val query = parsedDF.writeStream
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputPath)
      .outputMode("append")
      .start()

    // ✅ Print log to confirm stream started
    println("Avro file consumer streaming job started successfully.")

    query.awaitTermination()
  }
}
