package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FileBasedAvroEventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File-based Avro Event Consumer")
      .master("local[*]")
      .getOrCreate()

    val inputPath = "customer/event_output"    // Path where producer writes events
    val outputPath = "customer/tenant_data"    // Path the merge job reads from

    // Read the producer output with logical_date as integer days since epoch
    val df = spark.read.parquet(inputPath)

    // Convert logical_date from integer to string yyyy-MM-dd for partition folder naming
    val dfWithLogicalDateStr = df.withColumn(
      "logical_date_str",
      date_format(date_add(lit("1970-01-01"), col("logical_date")), "yyyy-MM-dd")
    )
    // Drop old integer logical_date and rename new string column to logical_date
    val finalDF = dfWithLogicalDateStr.drop("logical_date")
      .withColumnRenamed("logical_date_str", "logical_date")

    // Write partitioned Parquet files using string formatted logical_date for folders like logical_date=2025-04-03
    finalDF.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(outputPath)

    println(s"✅ Consumer job wrote data partitioned by tenant_id, partition_id, logical_date (string) to $outputPath")

    spark.stop()
  }
}