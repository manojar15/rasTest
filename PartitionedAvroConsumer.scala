package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object FileBasedAvroEventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File-Based Avro Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val logicalDate = if (args.length > 0) args(0) else "2025-04-03"
    val tenantDataPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val avroInputPath = s"file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/*/logical_date=$logicalDate"

    val wrapperSchemaJson = new String(
      Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")),
      StandardCharsets.UTF_8
    )

    val payloadSchemaJson = new String(
      Files.readAllBytes(Paths.get("src/main/avro/Customer.avsc")),
      StandardCharsets.UTF_8
    )

    val avroDF = spark.read.format("avro").load(avroInputPath)

    val withDecodedPayload = avroDF
      .withColumn("payload", from_avro(col("payload"), payloadSchemaJson))
      .withColumn("tenant_id", col("header.tenant_id"))
      .withColumn("customer_id", col("header.entity_id"))
      .withColumn("event_type", col("header.event_type"))
      .withColumn("partition_id", split(col("header.entity_id"), "_")(0))
      .withColumn("logical_date", col("header.logical_date"))
      .withColumn("event_timestamp", col("header.event_timestamp"))
      .select(
        col("partition_id"),
        col("tenant_id"),
        col("customer_id"),
        col("event_type"),
        col("logical_date"),
        col("event_timestamp"),
        col("payload.*")
      )

    withDecodedPayload.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .mode("overwrite")  // or "append" if you prefer incremental writes
      .format("parquet")
      .save(tenantDataPath)

    spark.stop()
  }
}
