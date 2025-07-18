package rastest

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
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

    val baseInputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/2025-04-03"
    val existingInputPath = s"$baseInputPath/existing"
    val newInputPath = s"$baseInputPath/new"
    val outputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"

    // Load your wrapper schema JSON (CustomerEvent.avsc)
    val wrapperSchemaJson = new String(
      Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")),
      StandardCharsets.UTF_8
    )

    // Read all Avro event files (existing + new)
    val existingDF = spark.read.format("avro").load(existingInputPath)
    val newDF = spark.read.format("avro").load(newInputPath)
    val eventsDF = existingDF.union(newDF)

    // Parse the "payload" bytes column inside the wrapper event,
    // based on the event_type in header
    // So first parse wrapper to struct, then parse payload bytes to actual schema

    val parsedEventsDF = eventsDF
      .select(
        from_avro(F.col("header").cast("binary"), wrapperSchemaJson).alias("header"),
        F.col("payload")
      )
      .selectExpr(
        "header.event_type as event_type",
        "header.entity_id as customer_id",
        "header.tenant_id as tenant_id",
        "header.logical_date as logical_date",
        "payload"
      )

    // Load individual payload schemas for decoding "payload" bytes
    val nameSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/NamePayload.avsc")), StandardCharsets.UTF_8)
    val addressSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/AddressPayload.avsc")), StandardCharsets.UTF_8)
    val idSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/IdentificationPayload.avsc")), StandardCharsets.UTF_8)

    // Helper function to decode payload bytes per event type
    def decodePayload(df: DataFrame, eventType: String, schemaJson: String) = {
      df.filter($"event_type" === eventType)
        .select(
          $"tenant_id",
          $"customer_id",
          $"logical_date",
          from_avro($"payload", schemaJson).alias("payload_parsed")
        )
        .selectExpr(
          "customer_id",
          "tenant_id",
          "logical_date",
          // extract all fields from payload_parsed struct as columns
          "payload_parsed.*"
        )
        .withColumn("event_type", F.lit(eventType))
    }

    // Decode each event type separately and union
    val nameEventsDF = decodePayload(parsedEventsDF, "NAME", nameSchemaJson)
    val addressEventsDF = decodePayload(parsedEventsDF, "ADDRESS", addressSchemaJson)
    val idEventsDF = decodePayload(parsedEventsDF, "IDENTIFICATION", idSchemaJson)

    val allEventsDF = nameEventsDF.union(addressEventsDF).union(idEventsDF)
      .withColumn("partition_id", F.split($"customer_id", "_").getItem(0))
      .withColumn("logical_date", F.to_date($"logical_date".cast("string"))) // convert logical_date int to date if needed

    // Write the data partitioned by tenant_id, partition_id, logical_date as Parquet
    allEventsDF.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet(outputPath)

    println("✅ Finished reading Avro events and writing partitioned Parquet for merge job.")
    spark.stop()
  }
}
