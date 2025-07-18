package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object FileBasedAvroEventConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Based Avro Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Paths and configs
    val baseInputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/2025-04-03"
    val baseOutputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    
    // Load schema json strings from your original avro schema files
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)
    val nameSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/NamePayload.avsc")), StandardCharsets.UTF_8)
    val addressSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/AddressPayload.avsc")), StandardCharsets.UTF_8)
    val idSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/IdentificationPayload.avsc")), StandardCharsets.UTF_8)

    // Read all Avro event files from both existing/new folders (adjust as needed)
    val inputDF = spark.read.format("avro")
      .load(s"$baseInputPath/existing/*.avro", s"$baseInputPath/new/*.avro")

    // inputDF schema: header: struct, payload: binary(bytes)
    // Extract header fields directly (struct)
    val withHeaderCols = inputDF
      .withColumn("customer_id", col("header.entity_id"))
      .withColumn("event_type", col("header.event_type"))
      .withColumn("logical_date", col("header.logical_date"))
      .withColumn("tenant_id", col("header.tenant_id"))
      .withColumn("partition_id", split(col("customer_id"), "_")(0))
      .withColumn("event_timestamp", col("header.event_timestamp"))
      .withColumn("payload_bytes", col("payload"))

    // Decode payload bytes based on event_type
    val decodedPayload = withHeaderCols.withColumn("payload_decoded",
      when(col("event_type") === "NAME", from_avro(col("payload_bytes"), nameSchemaJson))
       .when(col("event_type") === "ADDRESS", from_avro(col("payload_bytes"), addressSchemaJson))
       .when(col("event_type") === "IDENTIFICATION", from_avro(col("payload_bytes"), idSchemaJson))
       .otherwise(null)
    )

    // Flatten columns by merging header + payload fields
    val flattened = decodedPayload.select(
      col("partition_id"),
      col("tenant_id"),
      col("customer_id"),
      col("event_timestamp"),
      col("logical_date"),
      col("event_type"),
      // Flatten fields from payload_decoded struct
      col("payload_decoded.*")
    )

    // Write parquet partitioned by tenant_id, partition_id, logical_date (as required for merge job)
    flattened.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet(baseOutputPath)

    println("✅ Finished reading Avro events, decoding payloads, and writing partitioned parquet for merge job.")
    spark.stop()
  }
}
