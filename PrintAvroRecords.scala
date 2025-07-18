package rastest

import org.apache.spark.sql.SparkSession

object PrintAvroRecords {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Print Avro Records")
      .master("local[*]")
      .getOrCreate()

    val rawAvroPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/raw_events_avro"
    val snapshotAvroPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_avro"

    println("🔹 First 10 records from RAW EVENTS:")
    val rawDF = spark.read.format("avro").load(rawAvroPath)
    rawDF.show(10, truncate = false)

    println("🔹 First 10 records from CUSTOMER SNAPSHOT:")
    val snapshotDF = spark.read.format("avro").load(snapshotAvroPath)
    snapshotDF.show(10, truncate = false)

    spark.stop()
  }
}

