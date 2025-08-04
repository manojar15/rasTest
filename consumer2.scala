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

    // Load Avro schema JSON for deserialization
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)

    // Read Avro files with permissive mode on file read (only helps file-level corruption)
    val dfModified = spark.read
      .option("mode", "PERMISSIVE")
      .format("avro")
      .load(s"$avroInputBase/modified")

    val dfNew = spark.read
      .option("mode", "PERMISSIVE")
      .format("avro")
      .load(s"$avroInputBase/new")

    // Union datasets and pre-filter empty or null payload_bytes (common source of problems)
    val avroDF = dfModified.unionByName(dfNew)
      .filter(col("payload_bytes").isNotNull && length(col("payload_bytes")) > 0)

    // Deserialize Avro bytes into struct column; filter out null deserialization results (malformed)
    val parsed = avroDF
      .withColumn("event", from_avro(col("payload_bytes"), wrapperSchemaJson))
      .filter(col("event").isNotNull) // drop records where deserialization failed and returned null
      .select(
        col("partition_id"),
        col("tenant_id"),
        col("customer_id"),
        col("event.header.event_timestamp").as("event_timestamp"),
        col("event.header.logical_date").as("logical_date"),
        col("event.header.event_type").as("event_type")
        // Add other nested fields here if needed
      )

    // Write out fully decoded data in Parquet with proper partitioning
    parsed.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .save(baseOutputPath)

    println(s"✅ Avro events fully deserialized and written to Parquet partitions at $baseOutputPath")

    spark.stop()
  }
}
