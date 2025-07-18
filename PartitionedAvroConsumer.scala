package rastest

import org.apache.spark.sql.SparkSession
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

    // Input and output paths
    val avroInputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output" // folder with avro files (new + existing)
    val baseOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val checkpointLocation = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/checkpoints_partitioned_v2"

    // Load CustomerEvent schema JSON text
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)

    // For streaming from Avro files, Spark requires explicit schema (cannot infer automatically)
    // We'll read files as binary 'value' column, then parse with from_avro using schema string.
    import org.apache.spark.sql.types._

    // Define a schema with a binary column 'value' because each file has full Avro records
    val avroBinarySchema = StructType(Seq(
      StructField("value", BinaryType)
    ))

    val rawDF = spark.readStream
      .format("avro")
      .schema(avroBinarySchema) // Specify schema explicitly
      .load(avroInputPath)

    val parsed = rawDF
      // Deserialize Avro binary using your CustomerEvent schema
      .withColumn("event", from_avro(col("value"), wrapperSchemaJson))
      .selectExpr(
        "event.header.entity_id as customer_id",
        "event.header.event_type as event_type",
        "event.header.logical_date as logical_date_int",
        "event.header.tenant_id as tenant_id",
        "event.header.event_timestamp as event_timestamp"
      )
      // Convert logical_date int to date string
      .withColumn("logical_date", expr("date_format(date_add(to_date('1970-01-01'), logical_date_int), 'yyyy-MM-dd')"))
      // Partition ID is the prefix before underscore in customer_id (like "5_123456")
      .withColumn("partition_id", split(col("customer_id"), "_")(0))
      .select("partition_id", "tenant_id", "customer_id", "event_timestamp", "logical_date", "event_type")

    // Write output partitioned by tenant_id, partition_id, logical_date as parquet
    val query = parsed.writeStream
      .format("parquet")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .option("path", baseOutputPath)
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .start()

    println("Streaming query started, writing Parquet data...")

    query.awaitTermination()
  }
}
