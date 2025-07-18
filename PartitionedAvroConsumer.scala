package rastest

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object PartitionedAvroEventConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Paths - update these to your environment
    val inputAvroPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/2025-04-03"
    val outputBasePath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"

    // Load original Avro schema JSON strings
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)
    val nameSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/NamePayload.avsc")), StandardCharsets.UTF_8)
    val addressSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/AddressPayload.avsc")), StandardCharsets.UTF_8)
    val idSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/IdentificationPayload.avsc")), StandardCharsets.UTF_8)

    // Read wrapper Avro events as DataFrame
    val rawDf = spark.read.format("avro").load(inputAvroPath)

    // Extract header fields & raw payload bytes
    val baseDf = rawDf
      .withColumn("header", F.col("header"))
      .withColumn("payload_bytes", F.col("payload").cast("binary"))
      .withColumn("customer_id", F.col("header.entity_id"))
      .withColumn("event_type", F.col("header.event_type"))
      .withColumn("logical_date", F.col("header.logical_date"))
      .withColumn("tenant_id", F.col("header.tenant_id"))
      .withColumn("partition_id", F.split(F.col("customer_id"), "_").getItem(0))
      .withColumn("event_timestamp", F.col("header.event_timestamp"))

    // Conditionally decode payload by event type, producing separate columns
    val withPayloads = baseDf
      .withColumn("namePayload",
        F.when(F.col("event_type") === "NAME", from_avro(F.col("payload_bytes"), nameSchemaJson))
         .otherwise(null))
      .withColumn("addressPayload",
        F.when(F.col("event_type") === "ADDRESS", from_avro(F.col("payload_bytes"), addressSchemaJson))
         .otherwise(null))
      .withColumn("idPayload",
        F.when(F.col("event_type") === "IDENTIFICATION", from_avro(F.col("payload_bytes"), idSchemaJson))
         .otherwise(null))

    // Coalesce into single struct column, taking whichever payload is non-null
    val finalDf = withPayloads.withColumn("payload_flat",
      F.coalesce(F.col("namePayload"), F.col("addressPayload"), F.col("idPayload"))
    )

    // Select only required columns for downstream merge job
    val outputDf = finalDf.select(
      "partition_id",
      "tenant_id",
      "customer_id",
      "event_timestamp",
      "logical_date",
      "event_type",
      "payload_flat"
    )

    // Write output partitioned for merge job consumption
    outputDf.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet(outputBasePath)

    println("✅ Avro consumer: flattened payloads written successfully.")
    spark.stop()
  }
}
