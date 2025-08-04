package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object PartitionedAvroEventConsumerBatch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro Event Consumer (Batch)")
      .master("local[*]")
      .getOrCreate()

    val avroInputBase = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    val baseOutputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"

    // Load the exact same Avro schema JSON used in producer
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)

    // Read Avro files
    val dfModified = spark.read.format("avro").load(s"$avroInputBase/modified")
    val dfNew      = spark.read.format("avro").load(s"$avroInputBase/new")

    val avroDF = dfModified.unionByName(dfNew)
      .filter(col("payload_bytes").isNotNull && length(col("payload_bytes")) > 0) // Filter invalid payload bytes

    // Deserialize nested Avro payload_bytes column using from_avro and handle corrupt records by filtering nulls
    val parsed = avroDF
      .withColumn("event", from_avro(col("payload_bytes"), wrapperSchemaJson))
      .filter(col("event").isNotNull) // filter out deserialization failures
      .select(
        col("partition_id"),
        col("tenant_id"),
        col("customer_id"),
        col("event.header.event_timestamp").as("event_timestamp"),
        col("event.header.logical_date").as("logical_date"),
        col("event.header.event_type").as("event_type")
        // Extract additional nested fields here as needed
      )

    // Write structured, fully decoded data as partitioned Parquet files
    parsed.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .save(baseOutputPath)

    println(s"✅ Avro events fully decoded and saved in Parquet partitions at $baseOutputPath")

    spark.stop()
  }
}
