import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

// Load Avro schema JSON string
val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)

val rawDF = spark.read.format("avro").load(s"$avroInputBase/modified")

// Assuming rawDF has a column 'wrapper_bytes' (or 'payload_bytes') containing raw Avro bytes
// Convert the bytes column to a struct columns using from_avro
val parsedDF = rawDF.withColumn("event", from_avro(col("wrapper_bytes"), wrapperSchemaJson))

// Now explode struct fields into top-level columns, for example:
val flatDF = parsedDF.select(
  col("partition_id"),
  col("tenant_id"),
  col("customer_id"),
  col("event.header.event_timestamp").as("event_timestamp"),
  col("event.header.logical_date").as("logical_date"),
  col("event.header.event_type").as("event_type"),
  // include other nested fields as needed
)

// Write flatDF to Parquet for your merge job to consume
flatDF.write
  .mode("overwrite")
  .partitionBy("tenant_id", "partition_id", "logical_date")
  .format("parquet")
  .save(baseOutputPath)
