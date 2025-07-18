package rastest

import org.apache.spark.sql.SparkSession

object SampleCustomerSnapshot {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sample Initial Customer Snapshot")
      .master("local[*]")
      .getOrCreate()

    val snapshotPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_avro"

    val df = spark.read.format("avro").load(snapshotPath)
    df.printSchema()

    df.show(20, truncate = false)  // Show 20 full records

    spark.stop()
  }
}

