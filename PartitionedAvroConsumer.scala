package rastest

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object PartitionedAvroFileEventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro File Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Input: Folder where new producer writes Avro files partitioned by logical_date/new and logical_date/existing
    val baseInputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    // Output path matching old Kafka consumer output
    val baseOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    // Checkpoint location (for streaming, but here used for consistency)
    val checkpointLocation = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/checkpoints_partitioned_v2"

    // Load wrapper Avro schema JSON (CustomerEvent.avsc)
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)

    // Read all Avro files from both 'existing' and 'new' folders recursively under the baseInputPath
    val avroDF = spark.read.format("avro")
      .load(s"$baseInputPath/*/*/*.avro")  // matches avro_output/2025-04-03/existing/*.avro and avro_output/2025-04-03/new/*.avro

    // Parse Avro payload bytes column to get nested payload record
    // The schema has "header" (record) and "payload" (bytes) — payload bytes need decoding separately if needed, but 
    // for this consumer's output partitioning and merging, we mainly need header fields

    // Extract event from the Avro record column (already parsed by spark-avro)
    // `avroDF` schema: header (struct), payload (binary), etc.

    // We'll extract fields:
    // - customer_id = header.entity_id
    // - event_type = header.event_type
    // - logical_date = header.logical_date (int with logicalType=date)
    // - tenant_id = header.tenant_id (int)
    // - partition_id = extract from customer_id (split by "_", take first)
    // - event_timestamp = header.event_timestamp

    val parsedDF = avroDF
      .withColumn("customer_id", $"header.entity_id")
      .withColumn("event_type", $"header.event_type")
      .withColumn("logical_date_int", $"header.logical_date")  // Int representing days since epoch
      .withColumn("tenant_id", $"header.tenant_id".cast("string")) // cast to string for consistency with old consumer
      .withColumn("partition_id", F.split($"customer_id", "_")(0))
      .withColumn("event_timestamp", $"header.event_timestamp")

      // Convert logical_date_int (int) to Spark date
      .withColumn("logical_date", F.date_format(F.expr("date_add(to_date('1970-01-01'), logical_date_int)"), "yyyy-MM-dd"))
      // Keep original columns as needed
      .select(
        $"partition_id",
        $"tenant_id",
        $"customer_id",
        $"event_timestamp",
        $"logical_date",
        $"event_type",
        $"header",
        $"payload"
      )

    // Write the output partitioned by tenant_id, partition_id, logical_date as parquet
    parsedDF.write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet(baseOutputPath)

    println("✅ Partitioned Avro File Event Consumer completed successfully!")

    spark.stop()
  }
}
