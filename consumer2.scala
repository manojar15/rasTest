package rastest

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object PartitionedAvroEventFileConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro File Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Input and output directories
    val avroInputPath = "customer/avro_output"
    val baseOutputPath = "customer/tenant_data"
    val wrapperSchemaJson = new String(
      Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")),
      StandardCharsets.UTF_8
    )

    // Read Avro files written by producer
    val inputDF = spark.read
      .format("avro")
      .load(avroInputPath)

    // Deserialize payload using wrapper schema (via from_avro)
    val df = inputDF
      .withColumn("event", from_avro($"payload", wrapperSchemaJson))
      .withColumn("customer_id", $"header.entity_id")
      .withColumn("event_type", $"header.event_type")
      .withColumn("logical_date", $"header.logical_date")
      .withColumn("tenant_id", $"header.tenant_id")
      .withColumn("partition_id", split($"customer_id", "_").getItem(0))
      .withColumn("event_timestamp", $"header.event_timestamp")
      .select(
        $"partition_id",
        $"tenant_id",
        $"customer_id",
        $"event_timestamp",
        $"logical_date",
        $"event_type",
        $"payload" // original encoded payload
      )

    // Write output as partitioned Parquet in same structure
    df.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .mode("overwrite")
      .save(baseOutputPath)

    println(s"✅ Consumer completed. Output saved to: $baseOutputPath")
    spark.stop()
  }
}
