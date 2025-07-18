package rastest

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.io.Source

object FileBasedAvroEventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Based Avro Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Input and output paths
    val baseInputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/2025-04-03"
    val existingPath = s"$baseInputPath/existing"
    val newPath = s"$baseInputPath/new"

    val outputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"

    // Load original wrapper schema as JSON string
    val wrapperSchemaJson = Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString

    // Load payload schemas JSON strings
    val nameSchemaJson = Source.fromFile("src/main/avro/NamePayload.avsc").mkString
    val addressSchemaJson = Source.fromFile("src/main/avro/AddressPayload.avsc").mkString
    val idSchemaJson = Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString

    // Function to decode payload bytes based on event_type
    def decodePayloadColumn(eventTypeCol: Column, payloadBytesCol: Column): Column = {
      when(eventTypeCol === lit("NAME"), from_avro(payloadBytesCol, nameSchemaJson))
        .when(eventTypeCol === lit("ADDRESS"), from_avro(payloadBytesCol, addressSchemaJson))
        .when(eventTypeCol === lit("IDENTIFICATION"), from_avro(payloadBytesCol, idSchemaJson))
        .otherwise(lit(null))
    }

    // Read Avro wrapper files from both existing and new folders
    val existingDF = spark.read.format("avro").load(existingPath)
    val newDF = spark.read.format("avro").load(newPath)

    // Union both
    val rawDF = existingDF.union(newDF)

    // The Avro wrapper schema has:
    // header: struct { event_timestamp, logical_date (int), event_type, tenant_id, entity_id }
    // payload: binary (bytes)

    val explodedDF = rawDF
      .withColumn("header", col("header"))
      .withColumn("payload_bytes", col("payload"))
      .withColumn("event_type", col("header.event_type"))
      .withColumn("tenant_id", col("header.tenant_id"))
      .withColumn("customer_id", col("header.entity_id"))
      .withColumn("logical_date_int", col("header.logical_date"))
      .withColumn("event_timestamp", col("header.event_timestamp"))
      // convert logical_date int (epoch days) to string date
      .withColumn("logical_date", expr("date_add('1970-01-01', logical_date_int)"))
      // extract partition_id from customer_id (assuming format partitionId_rest)
      .withColumn("partition_id", split(col("customer_id"), "_")(0))
      // decode payload bytes based on event_type
      .withColumn("payload_struct", decodePayloadColumn(col("event_type"), col("payload_bytes")))

    // Now flatten the payload_struct fields into top level with aliasing
    // We must handle different payload schemas — flatten common fields and add nullable columns if needed

    // Define columns from payload structs (using select with aliases):
    val flattenedDF = explodedDF.select(
      col("partition_id"),
      col("tenant_id"),
      col("customer_id"),
      col("event_timestamp"),
      col("logical_date"),
      col("event_type"),

      // For NamePayload fields (nullable)
      col("payload_struct.customer_id").as("name_customer_id"),
      col("payload_struct.first").as("first"),
      col("payload_struct.middle").as("middle"),
      col("payload_struct.last").as("last"),

      // For AddressPayload fields (nullable)
      col("payload_struct.customer_id").as("address_customer_id"),
      col("payload_struct.type").as("address_type"),
      col("payload_struct.street").as("street"),
      col("payload_struct.city").as("city"),
      col("payload_struct.postal_code").as("postal_code"),
      col("payload_struct.country").as("country"),

      // For IdentificationPayload fields (nullable)
      col("payload_struct.customer_id").as("id_customer_id"),
      col("payload_struct.type").as("id_type"),
      col("payload_struct.number").as("id_number"),
      col("payload_struct.issuer").as("id_issuer")
    )

    // Write the flattened data as Parquet partitioned by tenant_id, partition_id, logical_date
    flattenedDF.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet(outputPath)

    println("✅ Consumer job finished writing Parquet for merge job.")

    spark.stop()
  }
}
