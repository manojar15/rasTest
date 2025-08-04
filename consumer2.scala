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

    val dfModified = spark.read.format("avro").load(s"$avroInputBase/modified")
    val dfNew      = spark.read.format("avro").load(s"$avroInputBase/new")
    val avroDF     = dfModified.unionByName(dfNew)

    // Deserialize payload_bytes column into nested struct `event`
    val parsed = avroDF
      .withColumn("event", from_avro(col("payload_bytes"), wrapperSchemaJson))
      .select(
        col("partition_id"),
        col("tenant_id"),
        col("customer_id"),
        col("event.event_timestamp").as("event_timestamp"),
        col("event.logical_date").as("logical_date"),
        col("event.event_type").as("event_type"),
        // include any other nested fields you need
      )

    // Write fully decoded data to Parquet partitions for merge job
    parsed.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .save(baseOutputPath)

    println(s"✅ Avro events fully deserialized and written to Parquet partitions at $baseOutputPath")

    spark.stop()
  }
}
