package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro._
import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object PartitionedAvroFileEventConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro File Event Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Input and output paths (adjust if needed)
    val baseInputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/*" // reading from existing avro folders (existing & new)
    val baseOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val checkpointLocation = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/checkpoints_partitioned_files"

    // Read Avro schema and convert to Spark StructType
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)
    val avroSchema = new Schema.Parser().parse(wrapperSchemaJson)
    val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    // Read Avro files as streaming source with schema
    val avroStreamDF = spark.readStream
      .format("avro")
      .schema(sparkSchema)
      .load(baseInputPath) // reads all Avro files under existing and new folders

    // Extract fields from the nested structure
    val parsed = avroStreamDF
      .withColumn("customer_id", col("header.entity_id"))
      .withColumn("event_type", col("header.event_type"))
      // 'logical_date' in Avro schema is int with logicalType 'date' (days since epoch)
      // We convert to actual Date by adding to epoch '1970-01-01'
      .withColumn("logical_date", expr("date_add(to_date('1970-01-01'), header.logical_date)"))
      .withColumn("tenant_id", col("header.tenant_id"))
      .withColumn("partition_id", split(col("customer_id"), "_")(0))
      .withColumn("event_timestamp", col("header.event_timestamp"))
      .select("partition_id", "tenant_id", "customer_id", "event_timestamp", "logical_date", "event_type", "payload")

    // Write output as Parquet with partitions matching next job expectations
    val query = parsed.writeStream
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .option("checkpointLocation", checkpointLocation)
      .option("path", baseOutputPath)
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
