package rastest

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object PartitionedAvroEventFileConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro File Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val avroInputPath = "customer/avro_output"
    val baseOutputPath = "customer/tenant_data"

    // Read Avro files written by producer
    val inputDF = spark.read
      .format("avro")
      .load(avroInputPath)

    val df = inputDF
      .withColumn("customer_id", $"header.entity_id")
      .withColumn("event_type", $"header.event_type")
      .withColumn("logical_date", $"header.logical_date")
      .withColumn("tenant_id", $"header.tenant_id")
      .withColumn("partition_id", split($"header.entity_id", "_").getItem(0))
      .withColumn("event_timestamp", $"header.event_timestamp")
      .select(
        $"partition_id",
        $"tenant_id",
        $"customer_id",
        $"event_timestamp",
        $"logical_date",
        $"event_type",
        $"payload" // raw binary - unchanged
      )

    // Save as Parquet with directory partitioning
    df.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .mode("overwrite")
      .save(baseOutputPath)

    println(s"✅ Consumer output saved to $baseOutputPath")
    spark.stop()
  }
}
