package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PartitionedAvroEventConsumerBatch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro Event Consumer (Batch, No Kafka)")
      .master("local[*]")
      .getOrCreate()

    val avroInputBase = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    val baseOutputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"

    // Read (union) both modified and new Avro event files written by the producer
    val dfModified = spark.read.format("avro").load(s"$avroInputBase/modified")
    val dfNew      = spark.read.format("avro").load(s"$avroInputBase/new")
    val avroDF     = dfModified.unionByName(dfNew)

    // Flatten (extract) header fields for partitioning and further usage
    val parsed = avroDF.select(
      col("header.tenant_id").as("tenant_id"),
      col("header.entity_id").as("customer_id"),
      col("header.event_timestamp").as("event_timestamp"),
      col("header.logical_date").as("logical_date"),
      col("header.event_type").as("event_type"),
      // You may add other header/payload fields if needed
      col("payload"),
      // Partition id logic: get from customer id prefix (adjust as needed)
      substring_index(col("header.entity_id"), "_", 1).as("partition_id")
    )

    // Write fully decoded data as Parquet, partitioned as required by your merge job
    parsed.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .save(baseOutputPath)

    println(s"✅ Avro events processed and written as Parquet partitions at $baseOutputPath")
    spark.stop()
  }
}
