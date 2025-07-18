package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PartitionedAvroConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    val baseOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val checkpointLocation = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/checkpoints_partitioned_v2"

    val df = spark.read
      .format("avro")
      .option("recursiveFileLookup", "true")
      .load(inputPath)

    val transformed = df
      .withColumn("partition_id", split(col("customer_id"), "_")(0))
      .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))

    transformed
      .write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .mode("overwrite")
      .save(baseOutputPath)

    println("✅ Avro files processed and written as partitioned Parquet.")
    spark.stop()
  }
}
