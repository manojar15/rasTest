package rastest

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object PartitionedAvroFileEventConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro File Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val baseInputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    val baseOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val checkpointLocation = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/checkpoints_partitioned_file_v2"

    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)

    // Read Avro files as a stream
    val avroDF = spark.readStream
      .format("avro")
      .load(s"$baseInputPath/*/*.avro")

    // Deserialize outer CustomerEvent
    val parsed = avroDF
      .withColumn("event", from_avro(col("value"), wrapperSchemaJson))
      .withColumn("customer_id", col("event.header.entity_id"))
      .withColumn("event_type", col("event.header.event_type"))
      .withColumn("logical_date", col("event.header.logical_date"))
      .withColumn("tenant_id", col("event.header.tenant_id"))
      .withColumn("partition_id", split(col("customer_id"), "_")(0))
      .withColumn("event_timestamp", col("event.header.event_timestamp"))
      .select("partition_id", "tenant_id", "customer_id", "event_timestamp", "logical_date", "event_type", "value")

    // Write to Parquet using partitioning like the Kafka consumer job
    parsed.writeStream
      .format("parquet")
      .option("checkpointLocation", checkpointLocation)
      .option("path", baseOutputPath)
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
