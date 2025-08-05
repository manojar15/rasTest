package rastest

import org.apache.spark.sql.SparkSession

object PartitionedParquetConsumerAlignMerge {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File-based Partitioned Parquet Consumer")
      .master("local[8]")
      .config("spark.sql.shuffle.partitions", "64")
      .getOrCreate()

    val inputPath = "customer/event_output" // Where the producer wrote events
    val outputPath = "customer/tenant_data" // Where the merge job reads events (or update as needed)

    // Read produced Parquet -- columns: customer_id, event_type, tenant_id, logical_date, event_timestamp, partition_id, value
    val df = spark.read
      .parquet(inputPath)

    // Optionally: filter/transform here if needed (for most, just pass-through is fine)

    // Write exactly as merge job expects
    df.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(outputPath)

    println(s"✅ Consumer wrote partitioned files for merge job to: $outputPath")

    spark.stop()
  }
}
