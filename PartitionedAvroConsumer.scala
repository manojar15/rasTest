package rastest

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object FileBasedAvroEventConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Based Avro Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Path where producer writes avro files (adjust your folder here)
    val inputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/2025-04-03/existing"

    // Load your main wrapper schema JSON string from original avsc file
    val wrapperSchemaJson = new String(
      Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")),
      StandardCharsets.UTF_8
    )

    // Read Avro files using Spark Avro format
    val rawEvents = spark.read
      .format("avro")
      .load(inputPath)

    // Your data has these columns: header (struct), payload (bytes)
    // To properly parse the payload, you need to deserialize the bytes with the right payload schema
    // Let's load all payload schemas

    val nameSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/NamePayload.avsc")), StandardCharsets.UTF_8)
    val addressSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/AddressPayload.avsc")), StandardCharsets.UTF_8)
    val identificationSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/IdentificationPayload.avsc")), StandardCharsets.UTF_8)

    // Extract fields from header
    val eventsWithHeader = rawEvents
      .withColumn("event_type", col("header.event_type"))
      .withColumn("entity_id", col("header.entity_id"))
      .withColumn("tenant_id", col("header.tenant_id"))
      .withColumn("logical_date", col("header.logical_date"))
      .withColumn("event_timestamp", col("header.event_timestamp"))
      // payload is a binary column (bytes), rename it for clarity
      .withColumnRenamed("payload", "payload_bytes")

    // Parse payload bytes based on event_type with when/otherwise
    val parsedPayload = eventsWithHeader.withColumn(
      "payload",
      when(col("event_type") === "NAME",
        from_avro(col("payload_bytes"), nameSchemaJson)
      ).when(col("event_type") === "ADDRESS",
        from_avro(col("payload_bytes"), addressSchemaJson)
      ).when(col("event_type") === "IDENTIFICATION",
        from_avro(col("payload_bytes"), identificationSchemaJson)
      ).otherwise(lit(null))
    )

    // Flatten for easy reading or processing
    val flattened = parsedPayload.select(
      col("tenant_id"),
      col("entity_id").as("customer_id"),
      col("event_timestamp"),
      col("logical_date"),
      col("event_type"),
      col("payload.*") // expand all fields of payload struct
    )

    // You can write it partitioned to feed next merge job, adjust path accordingly
    val outputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"

    flattened.write
      .mode("overwrite")
      .partitionBy("tenant_id", "customer_id", "logical_date") // partition by your keys
      .parquet(outputPath)

    println("✅ Consumer: Avro events parsed and saved successfully.")

    spark.stop()
  }
}
