package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FileBasedAvroEventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File-based Avro Event Consumer")
      .master("local[8]")
      .getOrCreate()

    val inputPath = "customer/event_output"
    val outputPath = "customer/tenant_data"

    val df = spark.read.parquet(inputPath)

    // Convert integer logical_date (days since epoch) to string yyyy-MM-dd format for partitioning
    val dfWithLogicalDateStr = df.withColumn(
      "logical_date",
      date_format(date_add(lit("1970-01-01"), col("logical_date")), "yyyy-MM-dd")
    )

    // Write partitioned Parquet files with converted logical_date string
    dfWithLogicalDateStr.write.mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .option("compression", "snappy")
      .parquet(outputPath)

    println(s"Consumer wrote data partitioned with string logical_date to $outputPath")
    spark.stop()
  }
}
