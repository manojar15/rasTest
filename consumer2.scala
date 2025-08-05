package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.avro.Schema
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericData}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}

object PartitionedAvroEventFileConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro File Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val avroInputPath = "customer/avro_output"
    val baseOutputPath = "customer/tenant_data"

    val wrapperSchemaStr = new String(
      Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")),
      StandardCharsets.UTF_8
    )
    val wrapperSchema = new Schema.Parser().parse(wrapperSchemaStr)

    // Read Avro files
    val inputDF = spark.read.format("avro").load(avroInputPath)

    // Convert ByteBuffer to Array[Byte]
    val dfWithPayloadBytes = inputDF.withColumn(
      "payload_bytes",
      col("payload").cast("binary") // cast ByteBuffer to binary type which maps to Array[Byte]
    )

    // Re-serialize each full record as 'value' column
    val encodedDF = dfWithPayloadBytes.mapPartitions { iter =>
      val writer = new GenericDatumWriter[GenericRecord](wrapperSchema)
      val factory = EncoderFactory.get()

      iter.map { row =>
        // Build GenericRecord with updated payload as byte array wrapped to ByteBuffer again
        val genericRecord = new GenericData.Record(wrapperSchema)
        wrapperSchema.getFields.forEach { field =>
          // Handle payload field with ByteBuffer wrapped from Array[Byte]
          if (field.name() == "payload") {
            val payloadArr = row.getAs[Array[Byte]]("payload_bytes")
            genericRecord.put(field.name(), ByteBuffer.wrap(payloadArr))
          } else {
            genericRecord.put(field.name(), row.getAs[Any](field.name()))
          }
        }

        val out = new ByteArrayOutputStream()
        val encoder: BinaryEncoder = factory.binaryEncoder(out, null)
        writer.write(genericRecord, encoder)
        encoder.flush()
        out.close()

        (
          row.getAs[String]("header.entity_id"),
          row.getAs[String]("header.event_type"),
          row.getAs[Int]("header.logical_date"),
          row.getAs[String]("header.tenant_id"),
          row.getAs[Long]("header.event_timestamp"),
          row.getAs[Array[Byte]]("payload_bytes"),
          out.toByteArray // 'value' column for merge job compatibility
        )
      }
    }.toDF(
      "customer_id",
      "event_type",
      "logical_date",
      "tenant_id",
      "event_timestamp",
      "payload",
      "value"
    ).withColumn("partition_id", split(col("customer_id"), "_").getItem(0))

    // Write output as partitioned Parquet files
    encodedDF.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("parquet")
      .mode("overwrite")
      .save(baseOutputPath)

    println(s"✅ Consumer output saved to $baseOutputPath")
    spark.stop()
  }
}
