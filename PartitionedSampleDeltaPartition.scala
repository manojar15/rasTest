package rastest

import org.apache.spark.sql.SparkSession

object SampleDeltaPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sample Delta Partition")
      .master("local[*]")
      .getOrCreate()

    val basePath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_delta"
    val logicalDate = "2025-03-31"
    val tenantId = if (args.length > 1) args(1) else "42"
    val path = s"$basePath/logical_date=$logicalDate"

    println(s"Reading delta data from: $path")

    val df = spark.read.format("avro").load(path)
    df.printSchema()

    println(s"Total rows: ${df.count()}")

    df.show(10, truncate = false)

    spark.stop()
  }
}